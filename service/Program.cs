using System;
using System.Collections.Generic;
using System.IO;
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
using Newtonsoft.Json.Linq;
using SparkplugNet.Core;
using SparkplugNet.VersionB;
using Google.Protobuf;
using SparkplugMetric = SparkplugNet.VersionB.Data.Metric;
using SparkplugPayload = SparkplugNet.VersionB.Data.Payload;
using SparkplugDataType = SparkplugNet.VersionB.Data.DataType;
using ProtoPayload = Org.Eclipse.Tahu.Protobuf.Payload;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<MqttPublisherService>();
var host = builder.Build();
host.Run();

public class MqttPublisherService : BackgroundService
{
    private readonly ILogger<MqttPublisherService> _logger;
    private readonly string _connectionString;
    private readonly string _mqttBroker;
    private readonly string _mqttUserName;
    private readonly string _mqttPassword;
    private readonly int _mqttPort = 1883;

    private IMqttClient? _mqttClient;
    private Func<CancellationToken, Task>? _connectAction;

    private readonly TimeSpan _pollDelay = TimeSpan.FromMilliseconds(500);
    private readonly Random _random = new();
    private ulong _sequenceNumber = 0;
    private readonly HashSet<string> _birthedDevices = new();
    private const string _groupId = "BUL";
    private const string _edgeNodeId = "JobBridge";
    private Guid _sessionId;
    private DateTime _sessionStartTime;

    public MqttPublisherService(ILogger<MqttPublisherService> logger, IConfiguration config)
    {
        _logger = logger;
        _connectionString = config.GetConnectionString("SqlServer")!;
        _mqttBroker = config["Mqtt:Broker"] ?? "localhost";
        _mqttUserName = config["Mqtt:Username"] ?? string.Empty;
        _mqttPassword = config["Mqtt:Password"] ?? string.Empty;
        if (int.TryParse(config["Mqtt:Port"], out var port))
        {
            _mqttPort = port;
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Create a new session ID for this connection
        _sessionId = Guid.NewGuid();
        _sessionStartTime = DateTime.UtcNow;
        _logger.LogInformation("Starting new Sparkplug session with ID: {SessionId}", _sessionId);

        // Ensure birth tracking table exists
        await EnsureBirthTrackingTableExists(stoppingToken);

        // Setup MQTT Client
        var factory = new MqttFactory();
        _mqttClient = factory.CreateMqttClient();

        var options = new MqttClientOptionsBuilder()
            .WithTcpServer(_mqttBroker, _mqttPort)
            .WithClientId($"SqlMqttBridge-{Environment.MachineName}")
            .WithCredentials(_mqttUserName, _mqttPassword)
            .WithCleanSession()
            .Build();

        // store a connect action that uses the built options (captured)
        _connectAction = ct => _mqttClient!.ConnectAsync(options, ct);

        // Ensure connected before entering the poll loop
        await ReconnectWithBackoff(_connectAction, stoppingToken);

        // Reset sequence number and publish NBIRTH (Node Birth) certificate first
        _sequenceNumber = 0;
        await PublishNodeBirthCertificate("BUL", "JobExecBridge", stoppingToken);

        // Discover and publish DBIRTH for all devices in the outbox
        await DiscoverAndPublishDeviceBirths("BUL", "JobExecBridge", _sessionId, stoppingToken);

        // Ensure disconnected handler also attempts reconnect using same connect action
        _mqttClient.DisconnectedAsync += async args =>
        {
            _logger.LogWarning(args?.Exception, "MQTT client disconnected, attempting reconnect...");
            try
            {
                if (_connectAction != null)
                {
                    await ReconnectWithBackoff(_connectAction, stoppingToken);

                    // Create new session ID for reconnection
                    var oldSessionId = _sessionId;
                    _sessionId = Guid.NewGuid();
                    _sessionStartTime = DateTime.UtcNow;
                    _logger.LogInformation("Reconnected with new Sparkplug session ID: {SessionId}", _sessionId);

                    // Clear old session's birth tracking
                    await ClearBirthTrackingForSession(oldSessionId, stoppingToken);

                    // Mark all unprocessed messages created before this session as processed
                    // to prevent sending stale DDATA with invalid sequence numbers
                    await MarkStaleMessagesAsProcessed(_sessionStartTime, stoppingToken);

                    // Reset sequence and republish birth certificates after reconnection
                    _sequenceNumber = 0;
                    _birthedDevices.Clear();
                    await PublishNodeBirthCertificate("BUL", "JobExecBridge", stoppingToken);
                    await DiscoverAndPublishDeviceBirths("BUL", "JobExecBridge", _sessionId, stoppingToken);
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
                    // Extract device name from topic and ensure DBIRTH was sent
                    var deviceName = ExtractDeviceNameFromTopic(topic);
                    if (!string.IsNullOrEmpty(deviceName) && !_birthedDevices.Contains(deviceName))
                    {
                        await PublishDeviceBirthCertificate("BUL", "JobExecBridge", deviceName, ct);
                        _birthedDevices.Add(deviceName);
                        await RecordDeviceBirth(deviceName, _sessionId, ct);
                    }

                    byte[] payloadBytes;

                    try
                    {
                        // Convert JSON payload to Sparkplug B format
                        var metrics = ConvertJsonToMetrics(payload);

                        // Serialize to protobuf using Google.Protobuf with sequence number
                        payloadBytes = SerializePayload(metrics, _sequenceNumber);
                        _sequenceNumber++;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to convert payload to Sparkplug B format for message {Id}", id);
                        // Skip this message
                        continue;
                    }

                    var message = new MqttApplicationMessageBuilder()
                        .WithTopic(topic)
                        .WithPayload(payloadBytes)
                        .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                        .WithRetainFlag(false)
                        .Build();

                    try
                    {
                        await _mqttClient!.PublishAsync(message, ct);
                        _logger.LogInformation("Published Sparkplug B message to {Topic}", topic);
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

    private async Task EnsureBirthTrackingTableExists(CancellationToken ct)
    {
        try
        {
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct);

            const string createTableSql = @"
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SparkplugBirthTracking')
                BEGIN
                    CREATE TABLE dbo.SparkplugBirthTracking (
                        DeviceName NVARCHAR(255) PRIMARY KEY,
                        LastBirthAt DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
                        SessionId UNIQUEIDENTIFIER NOT NULL
                    );
                    CREATE INDEX IX_SparkplugBirthTracking_SessionId ON dbo.SparkplugBirthTracking(SessionId);
                END";

            await using var cmd = new SqlCommand(createTableSql, connection);
            cmd.CommandTimeout = 30;
            await cmd.ExecuteNonQueryAsync(ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to ensure birth tracking table exists");
        }
    }

    private async Task ClearBirthTrackingForSession(Guid sessionId, CancellationToken ct)
    {
        try
        {
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct);

            const string deleteSql = "DELETE FROM dbo.SparkplugBirthTracking WHERE SessionId = @SessionId";

            await using var cmd = new SqlCommand(deleteSql, connection);
            cmd.Parameters.Add("@SessionId", SqlDbType.UniqueIdentifier).Value = sessionId;
            cmd.CommandTimeout = 30;
            await cmd.ExecuteNonQueryAsync(ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to clear birth tracking for session");
        }
    }

    private async Task MarkStaleMessagesAsProcessed(DateTime sessionStartTime, CancellationToken ct)
    {
        try
        {
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct);

            const string updateSql = @"
                UPDATE dbo.MQTTOutbox
                SET IsProcessed = 1, ProcessedAt = GETUTCDATE()
                WHERE IsProcessed = 0
                AND CreatedAt < @SessionStartTime";

            await using var cmd = new SqlCommand(updateSql, connection);
            cmd.Parameters.Add("@SessionStartTime", SqlDbType.DateTime2).Value = sessionStartTime;
            cmd.CommandTimeout = 30;

            var rowsAffected = await cmd.ExecuteNonQueryAsync(ct);

            if (rowsAffected > 0)
            {
                _logger.LogWarning("Marked {Count} stale messages as processed to prevent Sparkplug B protocol violations", rowsAffected);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to mark stale messages as processed");
        }
    }

    private async Task RecordDeviceBirth(string deviceName, Guid sessionId, CancellationToken ct)
    {
        try
        {
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct);

            const string upsertSql = @"
                MERGE dbo.SparkplugBirthTracking AS target
                USING (SELECT @DeviceName AS DeviceName, @SessionId AS SessionId) AS source
                ON target.DeviceName = source.DeviceName
                WHEN MATCHED THEN
                    UPDATE SET LastBirthAt = GETUTCDATE(), SessionId = @SessionId
                WHEN NOT MATCHED THEN
                    INSERT (DeviceName, SessionId, LastBirthAt)
                    VALUES (@DeviceName, @SessionId, GETUTCDATE());";

            await using var cmd = new SqlCommand(upsertSql, connection);
            cmd.Parameters.Add("@DeviceName", SqlDbType.NVarChar, 255).Value = deviceName;
            cmd.Parameters.Add("@SessionId", SqlDbType.UniqueIdentifier).Value = sessionId;
            cmd.CommandTimeout = 30;
            await cmd.ExecuteNonQueryAsync(ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to record device birth in database");
        }
    }

    private async Task DiscoverAndPublishDeviceBirths(string groupId, string edgeNodeId, Guid sessionId, CancellationToken ct)
    {
        try
        {
            // Query database to discover unique device names from topics
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct);

            const string sql = @"
                SELECT DISTINCT Topic
                FROM dbo.MQTTOutbox
                WHERE Topic LIKE 'spBv1.0/%/DDATA/%/%'";

            var deviceNames = new HashSet<string>();

            await using (var cmd = new SqlCommand(sql, connection))
            {
                cmd.CommandTimeout = 30;
                await using var reader = await cmd.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    var topic = reader.GetString(0);
                    var deviceName = ExtractDeviceNameFromTopic(topic);
                    if (!string.IsNullOrEmpty(deviceName))
                    {
                        deviceNames.Add(deviceName);
                    }
                }
            }

            // Publish DBIRTH for each unique device
            foreach (var deviceName in deviceNames)
            {
                if (!_birthedDevices.Contains(deviceName))
                {
                    await PublishDeviceBirthCertificate(groupId, edgeNodeId, deviceName, ct);
                    _birthedDevices.Add(deviceName);
                    await RecordDeviceBirth(deviceName, sessionId, ct);
                }
            }

            _logger.LogInformation("Discovered and published DBIRTH for {Count} devices", deviceNames.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to discover and publish device births");
        }
    }

    private static string? ExtractDeviceNameFromTopic(string topic)
    {
        // Expected format: spBv1.0/{group}/DDATA/{edgeNode}/{deviceName}
        var parts = topic.Split('/');
        if (parts.Length >= 5 && parts[2] == "DDATA")
        {
            return parts[4]; // Device name is the 5th part (index 4)
        }
        return null;
    }

    private async Task PublishNodeBirthCertificate(string groupId, string edgeNodeId, CancellationToken ct)
    {
        try
        {
            // Create NBIRTH (Node Birth) message - can be empty or contain node-level metrics
            var metrics = new List<SparkplugMetric>();

            var topic = $"spBv1.0/{groupId}/NBIRTH/{edgeNodeId}";

            // Serialize to protobuf with current sequence number (should be 0 for NBIRTH)
            var payloadBytes = SerializePayload(metrics, _sequenceNumber);
            _sequenceNumber++;

            var message = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(payloadBytes)
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .WithRetainFlag(false)
                .Build();

            await _mqttClient!.PublishAsync(message, ct);
            _logger.LogInformation("Published NBIRTH certificate to {Topic} with seq={Seq}", topic, _sequenceNumber - 1);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish NBIRTH certificate");
        }
    }

    private async Task PublishDeviceBirthCertificate(string groupId, string edgeNodeId, string deviceId, CancellationToken ct)
    {
        try
        {
            // Create DBIRTH (Device Birth) message with metric definitions
            var metrics = new List<SparkplugMetric>
            {
                new SparkplugMetric("woId", SparkplugDataType.String, "000000000"),
                new SparkplugMetric("operId", SparkplugDataType.String, "000000000"),
                new SparkplugMetric("seqNo", SparkplugDataType.Int32, 0),
                new SparkplugMetric("itemId", SparkplugDataType.String, "UNKNOWN"),
                new SparkplugMetric("timestamp", SparkplugDataType.String, DateTime.UtcNow.ToString("MM/dd/yyyy HH:mm"))
            };

            var topic = $"spBv1.0/{groupId}/DBIRTH/{edgeNodeId}/{deviceId}";

            // Serialize to protobuf with incremented sequence number
            var payloadBytes = SerializePayload(metrics, _sequenceNumber);
            _sequenceNumber++;

            var message = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(payloadBytes)
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .WithRetainFlag(false)
                .Build();

            await _mqttClient!.PublishAsync(message, ct);
            _logger.LogInformation("Published DBIRTH certificate to {Topic} with seq={Seq}", topic, _sequenceNumber - 1);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish DBIRTH certificate");
        }
    }

    private static byte[] SerializePayload(List<SparkplugMetric> metrics, ulong sequenceNumber)
    {
        // Create Sparkplug B protobuf payload
        var protoPayload = new ProtoPayload
        {
            Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Seq = sequenceNumber
        };

        // Convert metrics to protobuf format
        foreach (var metric in metrics)
        {
            var protoMetric = new ProtoPayload.Types.Metric
            {
                Name = metric.Name,
                Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Datatype = (uint)metric.DataType
            };

            // Set value based on data type
            switch (metric.DataType)
            {
                case SparkplugDataType.Int32:
                    protoMetric.IntValue = metric.Value != null ? Convert.ToUInt32(metric.Value) : 0;
                    break;
                case SparkplugDataType.String:
                    protoMetric.StringValue = metric.Value?.ToString() ?? string.Empty;
                    break;
                case SparkplugDataType.Boolean:
                    protoMetric.BooleanValue = metric.Value != null && Convert.ToBoolean(metric.Value);
                    break;
                case SparkplugDataType.Double:
                    protoMetric.DoubleValue = metric.Value != null ? Convert.ToDouble(metric.Value) : 0.0;
                    break;
                case SparkplugDataType.Float:
                    protoMetric.FloatValue = metric.Value != null ? Convert.ToSingle(metric.Value) : 0.0f;
                    break;
                default:
                    protoMetric.StringValue = metric.Value?.ToString() ?? string.Empty;
                    break;
            }

            protoPayload.Metrics.Add(protoMetric);
        }

        // Serialize using Google.Protobuf
        return protoPayload.ToByteArray();
    }

    private static List<SparkplugMetric> ConvertJsonToMetrics(string jsonPayload)
    {
        var metrics = new List<SparkplugMetric>();
        var json = JObject.Parse(jsonPayload);

        foreach (var property in json.Properties())
        {
            SparkplugMetric metric;

            // Determine data type and create metric
            switch (property.Value.Type)
            {
                case JTokenType.Integer:
                    metric = new SparkplugMetric(property.Name, SparkplugDataType.Int32, property.Value.Value<int>())
                    {
                        Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    };
                    break;
                case JTokenType.Float:
                    metric = new SparkplugMetric(property.Name, SparkplugDataType.Double, property.Value.Value<double>())
                    {
                        Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    };
                    break;
                case JTokenType.Boolean:
                    metric = new SparkplugMetric(property.Name, SparkplugDataType.Boolean, property.Value.Value<bool>())
                    {
                        Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    };
                    break;
                case JTokenType.String:
                default:
                    metric = new SparkplugMetric(property.Name, SparkplugDataType.String, property.Value.Value<string>() ?? string.Empty)
                    {
                        Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    };
                    break;
            }

            metrics.Add(metric);
        }

        return metrics;
    }

    private async Task PublishNodeDeathCertificate(string groupId, string edgeNodeId, CancellationToken ct)
    {
        try
        {
            // Create NDEATH (Node Death) message
            var topic = $"spBv1.0/{groupId}/NDEATH/{edgeNodeId}";

            // NDEATH should have the next sequence number in the sequence
            var payloadBytes = SerializePayload(new List<SparkplugMetric>(), _sequenceNumber);

            var message = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(payloadBytes)
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .WithRetainFlag(false)
                .Build();

            await _mqttClient!.PublishAsync(message, ct);
            _logger.LogInformation("Published NDEATH certificate to {Topic} with seq={Seq}", topic, _sequenceNumber);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish NDEATH certificate");
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            if (_mqttClient?.IsConnected == true)
            {
                // Publish NDEATH before disconnecting
                await PublishNodeDeathCertificate(_groupId, _edgeNodeId, cancellationToken);
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