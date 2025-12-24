using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MQTTnet.Client;
using MqttBridgeService.Configuration;

namespace MqttBridgeService.Services;

public class MqttClientManager
{
    private readonly ILogger<MqttClientManager> _logger;
    private readonly MqttSettings _settings;
    private readonly Random _random = new();
    private readonly SemaphoreSlim _reconnectLock = new(1, 1);
    private IMqttClient? _mqttClient;
    private Func<CancellationToken, Task>? _connectAction;

    public IMqttClient? Client => _mqttClient;
    public bool IsConnected => _mqttClient?.IsConnected ?? false;

    public MqttClientManager(ILogger<MqttClientManager> logger, MqttSettings settings)
    {
        _logger = logger;
        _settings = settings;
    }

    public void Initialize(string? lastWillTopic = null, byte[]? lastWillPayload = null)
    {
        var factory = new MqttFactory();
        _mqttClient = factory.CreateMqttClient();

        var optionsBuilder = new MqttClientOptionsBuilder()
            .WithTcpServer(_settings.Broker, _settings.Port)
            .WithClientId($"SqlMqttBridge-{Environment.MachineName}")
            .WithCredentials(_settings.Username, _settings.Password)
            .WithCleanSession();

        if (!string.IsNullOrEmpty(lastWillTopic) && lastWillPayload != null)
        {
            optionsBuilder
                .WithWillTopic(lastWillTopic)
                .WithWillPayload(lastWillPayload)
                .WithWillQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .WithWillRetain(false);

            _logger.LogInformation("Configured MQTT Last Will and Testament for {Topic}", lastWillTopic);
        }

        var options = optionsBuilder.Build();
        _connectAction = ct => _mqttClient.ConnectAsync(options, ct);
    }

    public async Task ConnectAsync(CancellationToken ct)
    {
        if (_connectAction == null)
        {
            throw new InvalidOperationException("MqttClientManager must be initialized before connecting");
        }

        await ReconnectWithBackoffAsync(_connectAction, ct);
    }

    public async Task ReconnectAsync(CancellationToken ct)
    {
        if (_connectAction == null)
        {
            _logger.LogWarning("No MQTT connect action available; skipping reconnect");
            return;
        }

        if (!await _reconnectLock.WaitAsync(0, ct))
        {
            _logger.LogDebug("Reconnection already in progress, skipping duplicate attempt");
            return;
        }

        try
        {
            await ReconnectWithBackoffAsync(_connectAction, ct);
        }
        finally
        {
            _reconnectLock.Release();
        }
    }

    public void SetDisconnectedHandler(Func<MqttClientDisconnectedEventArgs, Task> handler)
    {
        if (_mqttClient != null)
        {
            _mqttClient.DisconnectedAsync += handler;
        }
    }

    public async Task DisconnectAsync()
    {
        if (_mqttClient?.IsConnected == true)
        {
            await _mqttClient.DisconnectAsync();
        }
    }

    public async Task PublishAsync(MqttApplicationMessage message, CancellationToken ct)
    {
        if (_mqttClient == null)
        {
            throw new InvalidOperationException("MQTT client is not initialized");
        }

        await _mqttClient.PublishAsync(message, ct);
    }

    private async Task ReconnectWithBackoffAsync(Func<CancellationToken, Task> connectFunc, CancellationToken ct)
    {
        var attempt = 0;
        var consecutiveFailures = 0;

        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (_mqttClient?.IsConnected == true)
                {
                    return;
                }

                await connectFunc(ct);
                _logger.LogInformation("Connected to MQTT broker at {Broker}", _settings.Broker);
                return;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("connect/disconnect is pending"))
            {
                _logger.LogDebug("Connection attempt already in progress, backing off");
                attempt++;
                var backoffMs = Math.Min(1000 * (1 << attempt), 30_000) + _random.Next(0, 500);
                await Task.Delay(backoffMs, ct);
            }
            catch (Exception ex)
            {
                attempt++;
                consecutiveFailures++;
                var backoffMs = Math.Min(1000 * (1 << attempt), 30_000) + _random.Next(0, 500);

                if (consecutiveFailures <= 3 || consecutiveFailures % 10 == 0)
                {
                    _logger.LogWarning("Failed to connect to MQTT broker (attempt {Attempt}), retrying in {Backoff}ms: {Error}",
                        attempt, backoffMs, ex.Message);
                }
                else
                {
                    _logger.LogDebug(ex, "Failed to connect to MQTT broker (attempt {Attempt}), retrying in {Backoff}ms",
                        attempt, backoffMs);
                }

                await Task.Delay(backoffMs, ct);
            }
        }

        throw new OperationCanceledException(ct);
    }
}
