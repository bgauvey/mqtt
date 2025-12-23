using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using MqttBridgeService.Models;
using MqttBridgeService.Services;

namespace MqttBridgeService.Repositories;

public class OutboxRepository : IOutboxRepository
{
    private readonly string _connectionString;
    private readonly ILogger<OutboxRepository> _logger;
    private readonly SparkplugService _sparkplugService;

    public OutboxRepository(
        string connectionString,
        ILogger<OutboxRepository> logger,
        SparkplugService sparkplugService)
    {
        _connectionString = connectionString;
        _logger = logger;
        _sparkplugService = sparkplugService;
    }

    public async Task<List<OutboxMessage>> GetUnprocessedMessagesAsync(int count, CancellationToken ct)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);

        const string sql = @"
            SELECT TOP (@Count) Id, Topic, Payload
            FROM dbo.MQTTOutbox
            WHERE COALESCE(IsProcessed, 0) = 0
            ORDER BY CreatedAt";

        var messages = new List<OutboxMessage>();

        await using var cmd = new SqlCommand(sql, connection);
        cmd.CommandTimeout = 30;
        cmd.Parameters.Add("@Count", SqlDbType.Int).Value = count;

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            messages.Add(new OutboxMessage
            {
                Id = reader.GetInt64(0),
                Topic = reader.GetString(1),
                Payload = reader.GetString(2)
            });
        }

        return messages;
    }

    public async Task<List<(string Topic, string Payload)>> GetLatestMessagePerTopicAsync(CancellationToken ct)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);

        const string sql = @"
            SELECT t.Topic, t.Payload
            FROM dbo.MQTTOutbox t
            INNER JOIN (
                SELECT Topic, MAX(CreatedAt) as MaxCreatedAt
                FROM dbo.MQTTOutbox
                WHERE Topic LIKE 'spBv1.0/%/DDATA/%/%'
                GROUP BY Topic
            ) latest ON t.Topic = latest.Topic AND t.CreatedAt = latest.MaxCreatedAt";

        var messages = new List<(string Topic, string Payload)>();

        await using var cmd = new SqlCommand(sql, connection);
        cmd.CommandTimeout = 30;
        await using var reader = await cmd.ExecuteReaderAsync(ct);

        while (await reader.ReadAsync(ct))
        {
            messages.Add((reader.GetString(0), reader.GetString(1)));
        }

        return messages;
    }

    public async Task<string?> GetLatestPayloadForTopicAsync(string topic, CancellationToken ct)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);

        const string sql = @"
            SELECT TOP 1 Payload
            FROM dbo.MQTTOutbox
            WHERE Topic = @Topic
            ORDER BY CreatedAt DESC";

        await using var cmd = new SqlCommand(sql, connection);
        cmd.CommandTimeout = 30;
        cmd.Parameters.Add("@Topic", SqlDbType.NVarChar, 500).Value = topic;

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            return reader.GetString(0);
        }

        return null;
    }

    public async Task<TopicInfo?> GetFirstNodeTopicAsync(CancellationToken ct)
    {
        try
        {
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct);

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
                return _sparkplugService.ParseTopic(topic);
            }

            return null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to discover first node for LWT configuration");
            return null;
        }
    }

    public async Task<Dictionary<string, HashSet<string>>> GetAllNodeDeviceMappingsAsync(CancellationToken ct)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);

        const string sql = @"
            SELECT DISTINCT Topic
            FROM dbo.MQTTOutbox
            WHERE Topic LIKE 'spBv1.0/%/DDATA/%/%'";

        var nodeDeviceMap = new Dictionary<string, HashSet<string>>();

        await using var cmd = new SqlCommand(sql, connection);
        cmd.CommandTimeout = 30;
        await using var reader = await cmd.ExecuteReaderAsync(ct);

        while (await reader.ReadAsync(ct))
        {
            var topic = reader.GetString(0);
            var topicInfo = _sparkplugService.ParseTopic(topic);

            if (topicInfo.IsValid && topicInfo.NodeId != null)
            {
                if (!nodeDeviceMap.TryGetValue(topicInfo.NodeId, out var devices))
                {
                    devices = new HashSet<string>();
                    nodeDeviceMap[topicInfo.NodeId] = devices;
                }

                if (!string.IsNullOrEmpty(topicInfo.DeviceName))
                {
                    devices.Add(topicInfo.DeviceName);
                }
            }
        }

        return nodeDeviceMap;
    }

    public async Task MarkAsProcessedAsync(long id, CancellationToken ct)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);

        const string sql = @"
            UPDATE dbo.MQTTOutbox
            SET IsProcessed = 1, ProcessedAt = GETUTCDATE()
            WHERE Id = @Id AND COALESCE(IsProcessed, 0) = 0";

        await using var cmd = new SqlCommand(sql, connection);
        cmd.CommandTimeout = 30;
        cmd.Parameters.Add("@Id", SqlDbType.BigInt).Value = id;

        var rows = await cmd.ExecuteNonQueryAsync(ct);
        if (rows == 0)
        {
            _logger.LogInformation("Message {Id} was already processed by another worker", id);
        }
    }

    public async Task EnsureBirthTrackingTableExistsAsync(CancellationToken ct)
    {
        try
        {
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct);

            const string sql = @"
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SparkplugBirthTracking')
                BEGIN
                    CREATE TABLE dbo.SparkplugBirthTracking (
                        DeviceName NVARCHAR(255) PRIMARY KEY,
                        LastBirthAt DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
                        SessionId UNIQUEIDENTIFIER NOT NULL
                    );
                    CREATE INDEX IX_SparkplugBirthTracking_SessionId ON dbo.SparkplugBirthTracking(SessionId);
                END";

            await using var cmd = new SqlCommand(sql, connection);
            cmd.CommandTimeout = 30;
            await cmd.ExecuteNonQueryAsync(ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to ensure birth tracking table exists");
        }
    }

    public async Task RecordDeviceBirthAsync(string deviceName, Guid sessionId, CancellationToken ct)
    {
        try
        {
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct);

            const string sql = @"
                MERGE dbo.SparkplugBirthTracking AS target
                USING (SELECT @DeviceName AS DeviceName, @SessionId AS SessionId) AS source
                ON target.DeviceName = source.DeviceName
                WHEN MATCHED THEN
                    UPDATE SET LastBirthAt = GETUTCDATE(), SessionId = @SessionId
                WHEN NOT MATCHED THEN
                    INSERT (DeviceName, SessionId, LastBirthAt)
                    VALUES (@DeviceName, @SessionId, GETUTCDATE());";

            await using var cmd = new SqlCommand(sql, connection);
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

    public async Task ClearBirthTrackingForSessionAsync(Guid sessionId, CancellationToken ct)
    {
        try
        {
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct);

            const string sql = "DELETE FROM dbo.SparkplugBirthTracking WHERE SessionId = @SessionId";

            await using var cmd = new SqlCommand(sql, connection);
            cmd.Parameters.Add("@SessionId", SqlDbType.UniqueIdentifier).Value = sessionId;
            cmd.CommandTimeout = 30;
            await cmd.ExecuteNonQueryAsync(ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to clear birth tracking for session");
        }
    }
}
