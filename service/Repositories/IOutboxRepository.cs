using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MqttBridgeService.Models;

namespace MqttBridgeService.Repositories;

public interface IOutboxRepository
{
    Task<List<OutboxMessage>> GetUnprocessedMessagesAsync(int count, CancellationToken ct);
    Task<List<(string Topic, string Payload)>> GetLatestMessagePerTopicAsync(CancellationToken ct);
    Task<string?> GetLatestPayloadForTopicAsync(string topic, CancellationToken ct);
    Task<TopicInfo?> GetFirstNodeTopicAsync(CancellationToken ct);
    Task<Dictionary<string, HashSet<string>>> GetAllNodeDeviceMappingsAsync(CancellationToken ct);
    Task MarkAsProcessedAsync(long id, CancellationToken ct);
    Task EnsureBirthTrackingTableExistsAsync(CancellationToken ct);
    Task RecordDeviceBirthAsync(string deviceName, Guid sessionId, CancellationToken ct);
    Task ClearBirthTrackingForSessionAsync(Guid sessionId, CancellationToken ct);
}
