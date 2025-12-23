namespace MqttBridgeService.Configuration;

public class MqttSettings
{
    public string Broker { get; set; } = "localhost";
    public int Port { get; set; } = 1883;
    public string Username { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
    public int PollDelayMs { get; set; } = 500;
    public int DataRefreshIntervalSeconds { get; set; } = 30;
}
