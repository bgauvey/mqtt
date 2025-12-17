using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
using MQTTnet;
using MQTTnet.Client;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<MqttPublisherService>();
var host = builder.Build();
host.Run();

public class MqttPublisherService : BackgroundService
{
    private readonly ILogger<MqttPublisherService> _logger;
    private readonly string _connectionString;
    private readonly string _mqttBroker;
    private IMqttClient? _mqttClient;
    private Func<CancellationToken, Task>? _connectAction;

    private readonly TimeSpan _pollDelay = TimeSpan.FromMilliseconds(500);
    private readonly Random _random = new();

    public MqttPublisherService(ILogger<MqttPublisherService> logger, IConfiguration config)
    {
        _logger = logger;
        _connectionString = config.GetConnectionString("SqlServer")!;
        _mqttBroker = config["Mqtt:Broker"] ?? "localhost";
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Setup MQTT Client
        var factory = new MqttFactory();
        _mqttClient = factory.CreateMqttClient();


        var options = new MqttClientOptionsBuilder()
            .WithTcpServer(_mqttBroker, 1883)
            .WithClientId($"SqlMqttBridge-{Environment.MachineName}")
            .WithCleanSession()
            .Build();

        // store a connect action that uses the built options (captured)
        _connectAction = ct => _mqttClient!.ConnectAsync(options, ct);

        // Ensure connected before entering the poll loop
        await ReconnectWithBackoff(_connectAction, stoppingToken);

        // Ensure disconnected handler also attempts reconnect using same connect action
        _mqttClient.DisconnectedAsync += async args =>
        {
            _logger.LogWarning(args?.Exception, "MQTT client disconnected, attempting reconnect...");
            try
            {
                if (_connectAction != null)
                {
                    await ReconnectWithBackoff(_connectAction, stoppingToken);
                }
            }
            catch (OperationCanceledException)
            {
                // shutting down
            }
        };

        // Poll loop
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProcessOutboxMessages(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing outbox");
            }

            await Task.Delay(_pollDelay, stoppingToken);
        }
    }

    private async Task ReconnectWithBackoff(Func<CancellationToken, Task> connectFunc, CancellationToken ct)
    {
        var attempt = 0;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (_mqttClient?.IsConnected == true)
                {
                    return;
                }

                await connectFunc(ct);
                _logger.LogInformation("Connected to MQTT broker at {Broker}", _mqttBroker);
                return;
            }
            catch (Exception ex)
            {
                attempt++;
                var backoffMs = Math.Min(1000 * (1 << attempt), 30_000) + _random.Next(0, 500);
                _logger.LogWarning(ex, "Failed to connect to MQTT broker, retrying in {Backoff}ms (attempt {Attempt})", backoffMs, attempt);
                try
                {
                    await Task.Delay(backoffMs, ct);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
            }
        }

        throw new OperationCanceledException(ct);
    }

    private async Task ProcessOutboxMessages(CancellationToken ct)
    {
        const int maxOpenAttempts = 3;
        var openAttempt = 0;

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await using var connection = new SqlConnection(_connectionString);
                await connection.OpenAsync(ct);

                // Fetch unprocessed messages
                const string selectSql = @"
                    SELECT TOP 100 Id, Topic, Payload 
                    FROM dbo.MQTTOutbox 
                    WHERE IsProcessed = 0 
                    ORDER BY CreatedAt";

                var messages = new List<(long Id, string Topic, string Payload)>();

                await using (var cmd = new SqlCommand(selectSql, connection))
                {
                    cmd.CommandTimeout = 30;
                    await using var reader = await cmd.ExecuteReaderAsync(ct);
                    while (await reader.ReadAsync(ct))
                    {
                        messages.Add((
                            reader.GetInt64(0),
                            reader.GetString(1),
                            reader.GetString(2)
                        ));
                    }
                }

                if (messages.Count == 0)
                {
                    return;
                }

                // Ensure MQTT is connected before publishing
                if (_mqttClient?.IsConnected != true)
                {
                    _logger.LogInformation("MQTT client not connected, attempting reconnect before publishing...");
                    if (_connectAction != null)
                    {
                        await ReconnectWithBackoff(_connectAction, ct);
                    }
                    else
                    {
                        _logger.LogWarning("No MQTT connect action available; skipping publish attempts");
                        return;
                    }
                }

                // Publish each message
                foreach (var (id, topic, payload) in messages)
                {
                    var message = new MqttApplicationMessageBuilder()
                        .WithTopic(topic)
                        .WithPayload(Encoding.UTF8.GetBytes(payload))
                        .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                        .WithRetainFlag(false)
                        .Build();

                    try
                    {
                        await _mqttClient!.PublishAsync(message, ct);
                        _logger.LogInformation("Published to {Topic}", topic);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to publish message {Id} to {Topic}", id, topic);
                        // Skip marking this message processed so it can be retried later
                        continue;
                    }

                    // Mark as processed with an optimistic check to avoid races
                    const string updateSql = @"
                        UPDATE dbo.MQTTOutbox 
                        SET IsProcessed = 1, ProcessedAt = GETUTCDATE() 
                        WHERE Id = @Id AND IsProcessed = 0";

                    await using var updateCmd = new SqlCommand(updateSql, connection);
                    updateCmd.CommandTimeout = 30;
                    updateCmd.Parameters.Add("@Id", SqlDbType.BigInt).Value = id;
                    var rows = await updateCmd.ExecuteNonQueryAsync(ct);
                    if (rows == 0)
                    {
                        _logger.LogInformation("Message {Id} was already processed by another worker", id);
                    }
                    else
                    {
                        _logger.LogDebug("Marked message {Id} processed", id);
                    }
                }

                return; // finished processing for now
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

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            if (_mqttClient?.IsConnected == true)
            {
                await _mqttClient.DisconnectAsync();
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error while disconnecting MQTT client during shutdown");
        }

        await base.StopAsync(cancellationToken);
    }
}