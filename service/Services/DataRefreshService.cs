using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MqttBridgeService.Configuration;
using MqttBridgeService.Repositories;

namespace MqttBridgeService.Services;

public class DataRefreshService
{
    private readonly ILogger<DataRefreshService> _logger;
    private readonly IOutboxRepository _repository;
    private readonly SparkplugService _sparkplugService;
    private readonly MqttClientManager _mqttClient;
    private readonly SequenceManager _sequenceManager;
    private readonly TimeSpan _refreshInterval;
    private DateTime _lastRefresh = DateTime.MinValue;

    public DataRefreshService(
        ILogger<DataRefreshService> logger,
        IOutboxRepository repository,
        SparkplugService sparkplugService,
        MqttClientManager mqttClient,
        SequenceManager sequenceManager,
        MqttSettings settings)
    {
        _logger = logger;
        _repository = repository;
        _sparkplugService = sparkplugService;
        _mqttClient = mqttClient;
        _sequenceManager = sequenceManager;
        _refreshInterval = TimeSpan.FromSeconds(settings.DataRefreshIntervalSeconds);
    }

    public bool ShouldRefresh()
    {
        return DateTime.UtcNow - _lastRefresh >= _refreshInterval;
    }

    public async Task RefreshDataAsync(CancellationToken ct)
    {
        try
        {
            if (!_mqttClient.IsConnected)
            {
                _logger.LogWarning("MQTT client not connected, skipping data refresh");
                return;
            }

            var latestMessages = await _repository.GetLatestMessagePerTopicAsync(ct);

            if (latestMessages.Count == 0)
            {
                _logger.LogDebug("No topics to refresh");
                return;
            }

            _logger.LogDebug("Refreshing data for {TopicCount} topics to prevent staleness", latestMessages.Count);

            foreach (var (topic, payload) in latestMessages)
            {
                var topicInfo = _sparkplugService.ParseTopic(topic);

                if (!topicInfo.IsValid || topicInfo.NodeId == null)
                {
                    _logger.LogWarning("Could not parse topic {Topic} for data refresh", topic);
                    continue;
                }

                try
                {
                    var metrics = _sparkplugService.ConvertJsonToMetrics(payload);
                    var seq = _sequenceManager.GetAndIncrementSequence(topicInfo.NodeId);
                    var payloadBytes = _sparkplugService.SerializePayload(metrics, seq);

                    var message = new MqttApplicationMessageBuilder()
                        .WithTopic(topic)
                        .WithPayload(payloadBytes)
                        .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                        .WithRetainFlag(false)
                        .Build();

                    await _mqttClient.PublishAsync(message, ct);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to refresh data for topic {Topic}", topic);
                }
            }

            _lastRefresh = DateTime.UtcNow;
            _logger.LogInformation("Completed data refresh for {TopicCount} topics", latestMessages.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to refresh latest data");
        }
    }

    public async Task PublishLatestStateAsync(CancellationToken ct)
    {
        try
        {
            var latestMessages = await _repository.GetLatestMessagePerTopicAsync(ct);
            _logger.LogInformation("Publishing latest state for {TopicCount} topics", latestMessages.Count);

            foreach (var (topic, payload) in latestMessages)
            {
                var topicInfo = _sparkplugService.ParseTopic(topic);

                if (!topicInfo.IsValid || topicInfo.NodeId == null)
                {
                    _logger.LogWarning("Could not parse topic {Topic} for state publishing", topic);
                    continue;
                }

                try
                {
                    var metrics = _sparkplugService.ConvertJsonToMetrics(payload);
                    var seq = _sequenceManager.GetAndIncrementSequence(topicInfo.NodeId);
                    var payloadBytes = _sparkplugService.SerializePayload(metrics, seq);

                    var message = new MqttApplicationMessageBuilder()
                        .WithTopic(topic)
                        .WithPayload(payloadBytes)
                        .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                        .WithRetainFlag(false)
                        .Build();

                    await _mqttClient.PublishAsync(message, ct);
                    _logger.LogDebug("Published latest state for {Topic}", topic);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to publish latest state for topic {Topic}", topic);
                }
            }

            _logger.LogInformation("Completed publishing latest state for all topics");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish latest state");
        }
    }
}
