// Program.cs
using MQTTnet;
using MQTTnet.Client;
using Microsoft.Data.SqlClient;
using System.Text;

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

        await _mqttClient.ConnectAsync(options, stoppingToken);
        _logger.LogInformation("Connected to MQTT broker at {Broker}", _mqttBroker);

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
            
            await Task.Delay(500, stoppingToken); // Poll every 500ms
        }
    }

    private async Task ProcessOutboxMessages(CancellationToken ct)
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
        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                messages.Add((
                    reader.GetInt64(0),
                    reader.GetString(1),
                    reader.GetString(2)
                ));
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

            await _mqttClient!.PublishAsync(message, ct);
            _logger.LogInformation("Published to {Topic}: {Payload}", topic, payload);

            // Mark as processed
            const string updateSql = @"
                UPDATE dbo.MQTTOutbox 
                SET IsProcessed = 1, ProcessedAt = GETUTCDATE() 
                WHERE Id = @Id";
            
            await using var updateCmd = new SqlCommand(updateSql, connection);
            updateCmd.Parameters.AddWithValue("@Id", id);
            await updateCmd.ExecuteNonQueryAsync(ct);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_mqttClient?.IsConnected == true)
        {
            await _mqttClient.DisconnectAsync();
        }
        await base.StopAsync(cancellationToken);
    }
}