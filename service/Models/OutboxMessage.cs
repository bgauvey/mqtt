namespace MqttBridgeService.Models;

public class OutboxMessage
{
    public long Id { get; set; }
    public string Topic { get; set; } = string.Empty;
    public string Payload { get; set; } = string.Empty;
}
