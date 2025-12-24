namespace MqttBridgeService.Models;

public class TopicInfo
{
    public string? GroupId { get; set; }
    public string? NodeId { get; set; }
    public string? DeviceName { get; set; }
    public string? MessageType { get; set; }

    public bool IsValid => !string.IsNullOrEmpty(GroupId) && !string.IsNullOrEmpty(NodeId) && !string.IsNullOrEmpty(MessageType);
}
