using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MqttBridgeService.Repositories;

namespace MqttBridgeService.Services;

public class OutboxProcessorService
{
    private readonly ILogger<OutboxProcessorService> _logger;
    private readonly IOutboxRepository _repository;
    private readonly SparkplugService _sparkplugService;
    private readonly MqttClientManager _mqttClient;
    private readonly SequenceManager _sequenceManager;
    private readonly BirthCertificateManager _birthManager;
    private readonly Random _random = new();

    public OutboxProcessorService(
        ILogger<OutboxProcessorService> logger,
        IOutboxRepository repository,
        SparkplugService sparkplugService,
        MqttClientManager mqttClient,
        SequenceManager sequenceManager,
        BirthCertificateManager birthManager)
    {
        _logger = logger;
        _repository = repository;
        _sparkplugService = sparkplugService;
        _mqttClient = mqttClient;
        _sequenceManager = sequenceManager;
        _birthManager = birthManager;
    }

    public async Task ProcessMessagesAsync(CancellationToken ct)
    {
        const int maxOpenAttempts = 3;
        var openAttempt = 0;

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var messages = await _repository.GetUnprocessedMessagesAsync(100, ct);

                if (messages.Count == 0)
                {
                    return;
                }

                // Ensure MQTT is connected
                if (!_mqttClient.IsConnected)
                {
                    _logger.LogInformation("MQTT client not connected, attempting reconnect before publishing...");
                    await _mqttClient.ReconnectAsync(ct);
                }

                // Ensure all nodes are birthed before processing messages
                await _birthManager.EnsureNodesAreBirthedAsync(messages, ct);

                // Publish each message
                foreach (var message in messages)
                {
                    var topicInfo = _sparkplugService.ParseTopic(message.Topic);

                    if (!topicInfo.IsValid || topicInfo.NodeId == null)
                    {
                        _logger.LogWarning("Could not extract group ID or node ID from topic {Topic}, skipping message {Id}",
                            message.Topic, message.Id);
                        continue;
                    }

                    byte[] payloadBytes;

                    try
                    {
                        var metrics = _sparkplugService.ConvertJsonToMetrics(message.Payload);
                        var seq = _sequenceManager.GetAndIncrementSequence(topicInfo.NodeId);
                        payloadBytes = _sparkplugService.SerializePayload(metrics, seq);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to convert payload to Sparkplug B format for message {Id}", message.Id);
                        continue;
                    }

                    var mqttMessage = new MqttApplicationMessageBuilder()
                        .WithTopic(message.Topic)
                        .WithPayload(payloadBytes)
                        .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                        .WithRetainFlag(false)
                        .Build();

                    try
                    {
                        await _mqttClient.PublishAsync(mqttMessage, ct);
                        _logger.LogDebug("Published Sparkplug B message to {Topic}", message.Topic);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to publish message {Id} to {Topic}", message.Id, message.Topic);
                        continue;
                    }

                    await _repository.MarkAsProcessedAsync(message.Id, ct);
                    _logger.LogDebug("Marked message {Id} processed", message.Id);
                }

                return;
            }
            catch (SqlException ex) when (openAttempt++ < maxOpenAttempts)
            {
                var waitMs = Math.Min(1000 * openAttempt, 5000) + _random.Next(0, 200);
                _logger.LogWarning(ex, "SQL error accessing outbox, retrying in {Wait}ms (attempt {Attempt})", waitMs, openAttempt);
                await Task.Delay(waitMs, ct);
                continue;
            }
        }
    }
}
