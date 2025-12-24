using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MqttBridgeService.Models;
using MqttBridgeService.Repositories;
using SparkplugMetric = SparkplugNet.VersionB.Data.Metric;

namespace MqttBridgeService.Services;

public class BirthCertificateManager
{
    private readonly ILogger<BirthCertificateManager> _logger;
    private readonly IOutboxRepository _repository;
    private readonly SparkplugService _sparkplugService;
    private readonly MqttClientManager _mqttClient;
    private readonly SequenceManager _sequenceManager;

    private readonly Dictionary<string, HashSet<string>> _birthedDevices = new();
    private readonly Dictionary<string, string> _nodeGroupMap = new();
    private readonly SemaphoreSlim _stateLock = new(1, 1);

    public BirthCertificateManager(
        ILogger<BirthCertificateManager> logger,
        IOutboxRepository repository,
        SparkplugService sparkplugService,
        MqttClientManager mqttClient,
        SequenceManager sequenceManager)
    {
        _logger = logger;
        _repository = repository;
        _sparkplugService = sparkplugService;
        _mqttClient = mqttClient;
        _sequenceManager = sequenceManager;
    }

    public async Task DiscoverAndPublishAllNodeBirthsAsync(Guid sessionId, CancellationToken ct)
    {
        try
        {
            var nodeDeviceMap = await _repository.GetAllNodeDeviceMappingsAsync(ct);

            foreach (var (nodeId, deviceNames) in nodeDeviceMap)
            {
                // Get groupId from the first device topic
                var groupId = await GetGroupIdForNodeAsync(nodeId, ct);
                if (string.IsNullOrEmpty(groupId))
                {
                    _logger.LogWarning("Could not determine group ID for node {NodeId}", nodeId);
                    continue;
                }

                SetNodeGroup(nodeId, groupId);

                // Reset sequence and publish NBIRTH
                _sequenceManager.ResetSequence(nodeId);
                await PublishNodeBirthCertificateAsync(groupId, nodeId, ct);

                // Publish DBIRTH for each device
                var birthedDevices = GetBirthedDevices(nodeId);
                foreach (var deviceName in deviceNames)
                {
                    if (!birthedDevices.Contains(deviceName))
                    {
                        await PublishDeviceBirthCertificateAsync(groupId, nodeId, deviceName, ct);
                        birthedDevices.Add(deviceName);
                        await _repository.RecordDeviceBirthAsync(deviceName, sessionId, ct);
                    }
                }

                _logger.LogDebug("Published NBIRTH and {DeviceCount} DBIRTH messages for node {NodeId} in group {GroupId}",
                    deviceNames.Count, nodeId, groupId);
            }

            _logger.LogDebug("Discovered and published births for {NodeCount} nodes", nodeDeviceMap.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to discover and publish node births");
        }
    }

    public async Task EnsureNodesAreBirthedAsync(List<OutboxMessage> messages, CancellationToken ct)
    {
        var nodeInfo = new Dictionary<string, (string GroupId, HashSet<string> Devices)>();

        foreach (var message in messages)
        {
            var topicInfo = _sparkplugService.ParseTopic(message.Topic);
            if (!topicInfo.IsValid || topicInfo.NodeId == null)
                continue;

            if (!nodeInfo.TryGetValue(topicInfo.NodeId, out var info))
            {
                info = (topicInfo.GroupId!, new HashSet<string>());
                nodeInfo[topicInfo.NodeId] = info;
            }

            if (!string.IsNullOrEmpty(topicInfo.DeviceName))
            {
                info.Devices.Add(topicInfo.DeviceName);
            }
        }

        foreach (var (nodeId, (groupId, devices)) in nodeInfo)
        {
            SetNodeGroup(nodeId, groupId);

            if (!HasNodeBeenBirthed(nodeId))
            {
                _sequenceManager.ResetSequence(nodeId);
                await PublishNodeBirthCertificateAsync(groupId, nodeId, ct);
                _logger.LogDebug("Published NBIRTH for node {NodeId} in group {GroupId}", nodeId, groupId);
            }

            var birthedDevices = GetBirthedDevices(nodeId);
            foreach (var deviceName in devices)
            {
                if (!birthedDevices.Contains(deviceName))
                {
                    await PublishDeviceBirthCertificateAsync(groupId, nodeId, deviceName, ct);
                    birthedDevices.Add(deviceName);
                    _logger.LogDebug("Published DBIRTH for device {DeviceName} on node {NodeId}", deviceName, nodeId);
                }
            }
        }
    }

    public async Task PublishNodeDeathCertificateAsync(string groupId, string nodeId, CancellationToken ct)
    {
        try
        {
            var topic = $"spBv1.0/{groupId}/NDEATH/{nodeId}";
            var seq = _sequenceManager.GetAndIncrementSequence(nodeId);
            var payloadBytes = _sparkplugService.SerializePayload(new List<SparkplugMetric>(), seq);

            var message = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(payloadBytes)
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .WithRetainFlag(false)
                .Build();

            await _mqttClient.PublishAsync(message, ct);
            _logger.LogDebug("Published NDEATH certificate to {Topic} with seq={Seq}", topic, seq);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish NDEATH certificate for node {NodeId}", nodeId);
        }
    }

    public void ClearBirthState()
    {
        _stateLock.Wait();
        try
        {
            _birthedDevices.Clear();
            _nodeGroupMap.Clear();
        }
        finally
        {
            _stateLock.Release();
        }
    }

    public string? GetNodeGroup(string nodeId)
    {
        _stateLock.Wait();
        try
        {
            return _nodeGroupMap.TryGetValue(nodeId, out var groupId) ? groupId : null;
        }
        finally
        {
            _stateLock.Release();
        }
    }

    public List<string> GetAllNodeIds()
    {
        _stateLock.Wait();
        try
        {
            return _nodeGroupMap.Keys.ToList();
        }
        finally
        {
            _stateLock.Release();
        }
    }

    private async Task PublishNodeBirthCertificateAsync(string groupId, string nodeId, CancellationToken ct)
    {
        try
        {
            var metrics = new List<SparkplugMetric>();
            var topic = $"spBv1.0/{groupId}/NBIRTH/{nodeId}";
            var seq = _sequenceManager.GetAndIncrementSequence(nodeId);
            var payloadBytes = _sparkplugService.SerializePayload(metrics, seq);

            var message = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(payloadBytes)
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .WithRetainFlag(false)
                .Build();

            await _mqttClient.PublishAsync(message, ct);
            _logger.LogDebug("Published NBIRTH certificate to {Topic} with seq={Seq}", topic, seq);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish NBIRTH certificate for node {NodeId}", nodeId);
        }
    }

    private async Task PublishDeviceBirthCertificateAsync(string groupId, string nodeId, string deviceId, CancellationToken ct)
    {
        try
        {
            var topic = $"spBv1.0/{groupId}/DDATA/{nodeId}/{deviceId}";
            var payload = await _repository.GetLatestPayloadForTopicAsync(topic, ct);

            List<SparkplugMetric> metrics;
            if (!string.IsNullOrEmpty(payload))
            {
                metrics = _sparkplugService.ExtractMetricDefinitionsFromJson(payload);
            }
            else
            {
                metrics = new List<SparkplugMetric>();
                _logger.LogWarning("No metrics found for device {DeviceId} on node {NodeId}, using empty DBIRTH", deviceId, nodeId);
            }

            var birthTopic = $"spBv1.0/{groupId}/DBIRTH/{nodeId}/{deviceId}";
            var seq = _sequenceManager.GetAndIncrementSequence(nodeId);
            var payloadBytes = _sparkplugService.SerializePayload(metrics, seq);

            var message = new MqttApplicationMessageBuilder()
                .WithTopic(birthTopic)
                .WithPayload(payloadBytes)
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .WithRetainFlag(false)
                .Build();

            await _mqttClient.PublishAsync(message, ct);
            _logger.LogDebug("Published DBIRTH certificate to {Topic} with seq={Seq} and {MetricCount} metrics",
                birthTopic, seq, metrics.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish DBIRTH certificate for device {DeviceId} on node {NodeId}", deviceId, nodeId);
        }
    }

    private async Task<string?> GetGroupIdForNodeAsync(string nodeId, CancellationToken ct)
    {
        var nodeDeviceMap = await _repository.GetAllNodeDeviceMappingsAsync(ct);
        if (nodeDeviceMap.TryGetValue(nodeId, out var devices) && devices.Any())
        {
            var topic = $"spBv1.0/%/DDATA/{nodeId}/{devices.First()}";
            var allTopics = await _repository.GetLatestMessagePerTopicAsync(ct);
            var matchingTopic = allTopics.FirstOrDefault(t => t.Topic.Contains($"/DDATA/{nodeId}/")).Topic;

            if (!string.IsNullOrEmpty(matchingTopic))
            {
                var topicInfo = _sparkplugService.ParseTopic(matchingTopic);
                return topicInfo.GroupId;
            }
        }
        return null;
    }

    private bool HasNodeBeenBirthed(string nodeId)
    {
        return _sequenceManager.HasSequence(nodeId);
    }

    private HashSet<string> GetBirthedDevices(string nodeId)
    {
        _stateLock.Wait();
        try
        {
            if (!_birthedDevices.TryGetValue(nodeId, out var devices))
            {
                devices = new HashSet<string>();
                _birthedDevices[nodeId] = devices;
            }
            return devices;
        }
        finally
        {
            _stateLock.Release();
        }
    }

    private void SetNodeGroup(string nodeId, string groupId)
    {
        _stateLock.Wait();
        try
        {
            _nodeGroupMap[nodeId] = groupId;
        }
        finally
        {
            _stateLock.Release();
        }
    }
}
