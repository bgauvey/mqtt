using System;
using System.Collections.Generic;
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
    private readonly Dictionary<string, byte> _sequenceNumbers = [];
    private readonly Dictionary<string, HashSet<string>> _birthedDevices = new();
    private readonly Dictionary<string, string> _nodeGroupMap = new(); // Maps nodeId to groupId
    private readonly SemaphoreSlim _stateLock = new(1, 1); // Protects shared state dictionaries
    private Guid _sessionId;

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
        _logger.LogInformation("Starting new Sparkplug session with ID: {SessionId}", _sessionId);

        // Ensure birth tracking table exists
        await EnsureBirthTrackingTableExists(stoppingToken);

        // Setup MQTT Client
        var factory = new MqttFactory();
        _mqttClient = factory.CreateMqttClient();

        // Discover the first node to set up Last Will and Testament
        var (firstGroupId, firstNodeId) = await DiscoverFirstNode(stoppingToken);

        var optionsBuilder = new MqttClientOptionsBuilder()
            .WithTcpServer(_mqttBroker, _mqttPort)
            .WithClientId($"SqlMqttBridge-{Environment.MachineName}")
            .WithCredentials(_mqttUserName, _mqttPassword)
            .WithCleanSession();

        // Add Last Will and Testament (LWT) for NDEATH if we have a node
        if (!string.IsNullOrEmpty(firstGroupId) && !string.IsNullOrEmpty(firstNodeId))
        {
            var ndeathTopic = $"spBv1.0/{firstGroupId}/NDEATH/{firstNodeId}";
            var ndeathPayload = SerializePayload(new List<SparkplugMetric>(), 0);

            optionsBuilder
                .WithWillTopic(ndeathTopic)
                .WithWillPayload(ndeathPayload)
                .WithWillQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .WithWillRetain(false);

            _logger.LogInformation("Configured MQTT Last Will and Testament for {Topic}", ndeathTopic);
        }
        else
        {
            _logger.LogWarning("No nodes found in database, skipping Last Will and Testament configuration");
        }

        var options = optionsBuilder.Build();

        // store a connect action that uses the built options (captured)
        _connectAction = ct => _mqttClient.ConnectAsync(options, ct);

        // Ensure connected before entering the poll loop
        await ReconnectWithBackoff(_connectAction, stoppingToken);

        // Discover all unique nodes from the database and publish NBIRTH/DBIRTH for each
        await DiscoverAndPublishAllNodeBirths(_sessionId, stoppingToken);

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
                    var reconnectTime = DateTime.UtcNow;
                    _logger.LogInformation("Reconnected with new Sparkplug session ID: {SessionId}", _sessionId);

                    // Clear old session's birth tracking
                    await ClearBirthTrackingForSession(oldSessionId, stoppingToken);

                    // IMPORTANT: Do NOT mark messages as processed here!
                    // Messages that arrived while we were disconnected need to be published.
                    // Only the original session start time matters for marking truly stale messages
                    // (messages that were in queue before the service first started).

                    // Reset sequence and republish birth certificates after reconnection
                    await _stateLock.WaitAsync(stoppingToken);
                    try
                    {
                        _sequenceNumbers.Clear();
                        _birthedDevices.Clear();
                        _nodeGroupMap.Clear();
                    }
                    finally
                    {
                        _stateLock.Release();
                    }

                    await DiscoverAndPublishAllNodeBirths(_sessionId, stoppingToken);
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
                    WHERE COALESCE(IsProcessed, 0) = 0
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

                // Before processing any messages, ensure all nodes are birthed
                await EnsureAllNodesAreBirthed(messages, ct);

                // Publish each message
                foreach (var (id, topic, payload) in messages)
                {
                    // Extract group ID, node ID and device name from topic
                    var (groupId, nodeId, deviceName) = ParseTopic(topic);

                    if (string.IsNullOrEmpty(groupId) || string.IsNullOrEmpty(nodeId))
                    {
                        _logger.LogWarning("Could not extract group ID or node ID from topic {Topic}, skipping message {Id}", topic, id);
                        continue;
                    }

                    // Note: NBIRTH and DBIRTH certificates are already published by EnsureAllNodesAreBirthed
                    // This ensures proper Sparkplug B ordering: NBIRTH -> DBIRTH -> DDATA

                    byte[] payloadBytes;

                    try
                    {
                        // Convert JSON payload to Sparkplug B format
                        var metrics = ConvertJsonToMetrics(payload);

                        // Serialize to protobuf using Google.Protobuf with sequence number
                        var seq = GetAndIncrementSequence(nodeId);
                        payloadBytes = SerializePayload(metrics, seq);
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
                        WHERE Id = @Id AND COALESCE(IsProcessed, 0) = 0";

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

    private async Task EnsureAllNodesAreBirthed(List<(long Id, string Topic, string Payload)> messages, CancellationToken ct)
    {
        // Group messages by node and collect all unique devices per node
        var nodeInfo = new Dictionary<string, (string GroupId, HashSet<string> Devices)>();

        foreach (var (_, topic, _) in messages)
        {
            var (groupId, nodeId, deviceName) = ParseTopic(topic);

            if (string.IsNullOrEmpty(groupId) || string.IsNullOrEmpty(nodeId))
                continue;

            if (!nodeInfo.TryGetValue(nodeId, out var info))
            {
                info = (groupId, new HashSet<string>());
                nodeInfo[nodeId] = info;
            }

            if (!string.IsNullOrEmpty(deviceName))
            {
                info.Devices.Add(deviceName);
            }
        }

        // For each node, ensure NBIRTH is sent, then DBIRTH for all devices
        foreach (var (nodeId, (groupId, devices)) in nodeInfo)
        {
            // Track the group for this node
            SetNodeGroup(nodeId, groupId);

            // Check if NBIRTH has been sent for this node
            if (!HasNodeBeenBirthed(nodeId))
            {
                // Reset sequence and publish NBIRTH
                ResetSequence(nodeId);
                await PublishNodeBirthCertificate(groupId, nodeId, ct);
                _logger.LogInformation("Published NBIRTH for node {NodeId} in group {GroupId}", nodeId, groupId);
            }

            // Ensure DBIRTH is sent for all devices on this node
            var birthedDevices = GetBirthedDevices(nodeId);
            foreach (var deviceName in devices)
            {
                if (!birthedDevices.Contains(deviceName))
                {
                    await PublishDeviceBirthCertificate(groupId, nodeId, deviceName, ct);
                    birthedDevices.Add(deviceName);
                    await RecordDeviceBirth(deviceName, _sessionId, ct);
                    _logger.LogInformation("Published DBIRTH for device {DeviceName} on node {NodeId}", deviceName, nodeId);
                }
            }
        }
    }

    private async Task<(string? GroupId, string? NodeId)> DiscoverFirstNode(CancellationToken ct)
    {
        try
        {
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct);

            // Query for any topic (regardless of IsProcessed) to discover a node for LWT
            const string sql = @"
                SELECT TOP 1 Topic
                FROM dbo.MQTTOutbox
                WHERE Topic LIKE 'spBv1.0/%/DDATA/%/%'";

            await using var cmd = new SqlCommand(sql, connection);
            cmd.CommandTimeout = 30;
            await using var reader = await cmd.ExecuteReaderAsync(ct);

            if (await reader.ReadAsync(ct))
            {
                var topic = reader.GetString(0);
                var (groupId, nodeId, _) = ParseTopic(topic);
                return (groupId, nodeId);
            }

            return (null, null);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to discover first node for LWT configuration");
            return (null, null);
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

    private async Task DiscoverAndPublishAllNodeBirths(Guid sessionId, CancellationToken ct)
    {
        try
        {
            // Query database to discover unique nodes and devices from topics
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct);

            // Query for ALL distinct topics (regardless of IsProcessed status) to discover nodes
            // This ensures we publish birth certificates on startup even if all messages are processed
            const string sql = @"
                SELECT DISTINCT Topic
                FROM dbo.MQTTOutbox
                WHERE Topic LIKE 'spBv1.0/%/DDATA/%/%'";

            var nodeDeviceMap = new Dictionary<string, HashSet<string>>();
            var nodeGroupMap = new Dictionary<string, string>(); // Track groupId for each node

            await using (var cmd = new SqlCommand(sql, connection))
            {
                cmd.CommandTimeout = 30;
                await using var reader = await cmd.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    var topic = reader.GetString(0);
                    _logger.LogDebug("Found topic in database: {Topic}", topic);
                    var (groupId, nodeId, deviceName) = ParseTopic(topic);

                    if (!string.IsNullOrEmpty(groupId) && !string.IsNullOrEmpty(nodeId))
                    {
                        // Track group for this node
                        nodeGroupMap[nodeId] = groupId;

                        if (!nodeDeviceMap.TryGetValue(nodeId, out var devices))
                        {
                            devices = [];
                            nodeDeviceMap[nodeId] = devices;
                        }

                        if (!string.IsNullOrEmpty(deviceName))
                        {
                            nodeDeviceMap[nodeId].Add(deviceName);
                        }
                    }
                    else
                    {
                        _logger.LogWarning("Topic {Topic} failed to parse: GroupId={GroupId}, NodeId={NodeId}, DeviceName={DeviceName}",
                            topic, groupId ?? "null", nodeId ?? "null", deviceName ?? "null");
                    }
                }
            }

            // For each node, publish NBIRTH then DBIRTH for all its devices
            foreach (var (nodeId, deviceNames) in nodeDeviceMap)
            {
                var groupId = nodeGroupMap[nodeId];

                // Track the group for this node
                SetNodeGroup(nodeId, groupId);

                // Reset sequence for this node and publish NBIRTH
                ResetSequence(nodeId);
                await PublishNodeBirthCertificate(groupId, nodeId, ct);

                // Publish DBIRTH for each device on this node
                var birthedDevices = GetBirthedDevices(nodeId);
                foreach (var deviceName in deviceNames)
                {
                    if (!birthedDevices.Contains(deviceName))
                    {
                        await PublishDeviceBirthCertificate(groupId, nodeId, deviceName, ct);
                        birthedDevices.Add(deviceName);
                        await RecordDeviceBirth(deviceName, sessionId, ct);
                    }
                }

                _logger.LogInformation("Published NBIRTH and {DeviceCount} DBIRTH messages for node {NodeId} in group {GroupId}",
                    deviceNames.Count, nodeId, groupId);
            }

            _logger.LogInformation("Discovered and published births for {NodeCount} nodes", nodeDeviceMap.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to discover and publish node births");
        }
    }

    private static (string? GroupId, string? NodeId, string? DeviceName) ParseTopic(string topic)
    {
        // Expected format: spBv1.0/{group}/DDATA/{edgeNode}/{deviceName}
        var parts = topic.Split('/');
        if (parts.Length >= 4 && parts[2] == "DDATA")
        {
            return (
                parts.Length >= 2 ? parts[1] : null,
                parts[3],
                parts.Length >= 5 ? parts[4] : null
            );
        }
        return (null, null, null);
    }

    private byte GetAndIncrementSequence(string nodeId)
    {
        _stateLock.Wait();
        try
        {
            if (!_sequenceNumbers.TryGetValue(nodeId, out var seq))
            {
                seq = 0;
                _sequenceNumbers[nodeId] = seq;
            }
            var current = seq;
            _sequenceNumbers[nodeId] = (byte)((seq + 1) % 256);
            return current;
        }
        finally
        {
            _stateLock.Release();
        }
    }

    private void ResetSequence(string nodeId)
    {
        _stateLock.Wait();
        try
        {
            _sequenceNumbers[nodeId] = 0;
        }
        finally
        {
            _stateLock.Release();
        }
    }

    private bool HasNodeBeenBirthed(string nodeId)
    {
        _stateLock.Wait();
        try
        {
            return _sequenceNumbers.ContainsKey(nodeId);
        }
        finally
        {
            _stateLock.Release();
        }
    }

    private HashSet<string> GetBirthedDevices(string nodeId)
    {
        _stateLock.Wait();
        try
        {
            if (!_birthedDevices.TryGetValue(nodeId, out var devices))
            {
                devices = [];
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

    private string? GetNodeGroup(string nodeId)
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

    private async Task PublishNodeBirthCertificate(string groupId, string edgeNodeId, CancellationToken ct)
    {
        try
        {
            // Create NBIRTH (Node Birth) message - can be empty or contain node-level metrics
            var metrics = new List<SparkplugMetric>();

            var topic = $"spBv1.0/{groupId}/NBIRTH/{edgeNodeId}";

            // Serialize to protobuf with current sequence number (should be 0 for NBIRTH)
            var seq = GetAndIncrementSequence(edgeNodeId);
            var payloadBytes = SerializePayload(metrics, seq);

            var message = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(payloadBytes)
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .WithRetainFlag(false)
                .Build();

            await _mqttClient!.PublishAsync(message, ct);
            _logger.LogInformation("Published NBIRTH certificate to {Topic} with seq={Seq}", topic, seq);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish NBIRTH certificate for node {NodeId}", edgeNodeId);
        }
    }

    private async Task PublishDeviceBirthCertificate(string groupId, string edgeNodeId, string deviceId, CancellationToken ct)
    {
        try
        {
            // Get a sample payload from the database to extract metric definitions
            var metrics = await GetMetricDefinitionsForDevice(groupId, edgeNodeId, deviceId, ct);

            if (metrics.Count == 0)
            {
                _logger.LogWarning("No metrics found for device {DeviceId} on node {NodeId}, using empty DBIRTH", deviceId, edgeNodeId);
            }

            var topic = $"spBv1.0/{groupId}/DBIRTH/{edgeNodeId}/{deviceId}";

            // Serialize to protobuf with incremented sequence number
            var seq = GetAndIncrementSequence(edgeNodeId);
            var payloadBytes = SerializePayload(metrics, seq);

            var message = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(payloadBytes)
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .WithRetainFlag(false)
                .Build();

            await _mqttClient!.PublishAsync(message, ct);
            _logger.LogInformation("Published DBIRTH certificate to {Topic} with seq={Seq} and {MetricCount} metrics",
                topic, seq, metrics.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish DBIRTH certificate for device {DeviceId} on node {NodeId}", deviceId, edgeNodeId);
        }
    }

    private async Task<List<SparkplugMetric>> GetMetricDefinitionsForDevice(string groupId, string edgeNodeId, string deviceId, CancellationToken ct)
    {
        try
        {
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct);

            // Get the most recent payload for this device to extract metric schema
            const string sql = @"
                SELECT TOP 1 Payload
                FROM dbo.MQTTOutbox
                WHERE Topic = @Topic
                ORDER BY CreatedAt DESC";

            var topic = $"spBv1.0/{groupId}/DDATA/{edgeNodeId}/{deviceId}";

            await using var cmd = new SqlCommand(sql, connection);
            cmd.CommandTimeout = 30;
            cmd.Parameters.Add("@Topic", SqlDbType.NVarChar, 500).Value = topic;

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
            {
                var payload = reader.GetString(0);
                return ExtractMetricDefinitionsFromJson(payload);
            }

            return new List<SparkplugMetric>();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get metric definitions for device {DeviceId}", deviceId);
            return new List<SparkplugMetric>();
        }
    }

    private static List<SparkplugMetric> ExtractMetricDefinitionsFromJson(string jsonPayload)
    {
        var metrics = new List<SparkplugMetric>();
        var json = JObject.Parse(jsonPayload);

        foreach (var property in json.Properties())
        {
            SparkplugMetric metric;

            // Determine data type and create metric with default/zero value for DBIRTH
            switch (property.Value.Type)
            {
                case JTokenType.Integer:
                    metric = new SparkplugMetric(property.Name, SparkplugDataType.Int32, 0)
                    {
                        Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    };
                    break;
                case JTokenType.Float:
                    metric = new SparkplugMetric(property.Name, SparkplugDataType.Double, 0.0)
                    {
                        Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    };
                    break;
                case JTokenType.Boolean:
                    metric = new SparkplugMetric(property.Name, SparkplugDataType.Boolean, false)
                    {
                        Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    };
                    break;
                case JTokenType.String:
                default:
                    metric = new SparkplugMetric(property.Name, SparkplugDataType.String, string.Empty)
                    {
                        Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    };
                    break;
            }

            metrics.Add(metric);
        }

        return metrics;
    }

    private static byte[] SerializePayload(List<SparkplugMetric> metrics, byte sequenceNumber)
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
            var seq = GetAndIncrementSequence(edgeNodeId);
            var payloadBytes = SerializePayload(new List<SparkplugMetric>(), seq);

            var message = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(payloadBytes)
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .WithRetainFlag(false)
                .Build();

            await _mqttClient!.PublishAsync(message, ct);
            _logger.LogInformation("Published NDEATH certificate to {Topic} with seq={Seq}", topic, seq);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish NDEATH certificate for node {NodeId}", edgeNodeId);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            if (_mqttClient?.IsConnected == true)
            {
                // Publish NDEATH for all active nodes before disconnecting
                List<string> nodeIds;
                await _stateLock.WaitAsync(cancellationToken);
                try
                {
                    nodeIds = [.. _sequenceNumbers.Keys];
                }
                finally
                {
                    _stateLock.Release();
                }

                foreach (var nodeId in nodeIds)
                {
                    var groupId = GetNodeGroup(nodeId);
                    if (!string.IsNullOrEmpty(groupId))
                    {
                        await PublishNodeDeathCertificate(groupId, nodeId, cancellationToken);
                    }
                }
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