using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MqttBridgeService.Configuration;
using MqttBridgeService.Repositories;
using MqttBridgeService.Services;

var builder = Host.CreateApplicationBuilder(args);

// Configure settings
var mqttSettings = new MqttSettings();
builder.Configuration.GetSection("Mqtt").Bind(mqttSettings);
builder.Services.AddSingleton(mqttSettings);

// Register services
builder.Services.AddSingleton<SparkplugService>();
builder.Services.AddSingleton<SequenceManager>();
builder.Services.AddSingleton<MqttClientManager>();

// Register repository
builder.Services.AddSingleton<IOutboxRepository>(sp =>
{
    var connectionString = builder.Configuration.GetConnectionString("SqlServer")!;
    var logger = sp.GetRequiredService<ILogger<OutboxRepository>>();
    var sparkplugService = sp.GetRequiredService<SparkplugService>();
    return new OutboxRepository(connectionString, logger, sparkplugService);
});

// Register managers and processors
builder.Services.AddSingleton<BirthCertificateManager>();
builder.Services.AddSingleton<DataRefreshService>();
builder.Services.AddSingleton<OutboxProcessorService>();

// Register hosted service
builder.Services.AddHostedService<MqttPublisherService>();

var host = builder.Build();
host.Run();

public class MqttPublisherService : BackgroundService
{
    private readonly ILogger<MqttPublisherService> _logger;
    private readonly MqttClientManager _mqttClient;
    private readonly IOutboxRepository _repository;
    private readonly BirthCertificateManager _birthManager;
    private readonly DataRefreshService _dataRefreshService;
    private readonly OutboxProcessorService _outboxProcessor;
    private readonly SequenceManager _sequenceManager;
    private readonly SparkplugService _sparkplugService;
    private readonly MqttSettings _settings;
    private Guid _sessionId;

    public MqttPublisherService(
        ILogger<MqttPublisherService> logger,
        MqttClientManager mqttClient,
        IOutboxRepository repository,
        BirthCertificateManager birthManager,
        DataRefreshService dataRefreshService,
        OutboxProcessorService outboxProcessor,
        SequenceManager sequenceManager,
        SparkplugService sparkplugService,
        MqttSettings settings)
    {
        _logger = logger;
        _mqttClient = mqttClient;
        _repository = repository;
        _birthManager = birthManager;
        _dataRefreshService = dataRefreshService;
        _outboxProcessor = outboxProcessor;
        _sequenceManager = sequenceManager;
        _sparkplugService = sparkplugService;
        _settings = settings;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _sessionId = Guid.NewGuid();
        _logger.LogInformation("Starting new Sparkplug session with ID: {SessionId}", _sessionId);

        await _repository.EnsureBirthTrackingTableExistsAsync(stoppingToken);

        // Setup Last Will and Testament
        var firstNode = await _repository.GetFirstNodeTopicAsync(stoppingToken);
        if (firstNode?.IsValid == true)
        {
            var ndeathTopic = $"spBv1.0/{firstNode.GroupId}/NDEATH/{firstNode.NodeId}";
            var ndeathPayload = _sparkplugService.SerializePayload(new(), 0);
            _mqttClient.Initialize(ndeathTopic, ndeathPayload);
            _logger.LogInformation("Configured MQTT Last Will and Testament for {Topic}", ndeathTopic);
        }
        else
        {
            _mqttClient.Initialize();
            _logger.LogWarning("No nodes found in database, skipping Last Will and Testament configuration");
        }

        // Setup disconnection handler
        _mqttClient.SetDisconnectedHandler(async args =>
        {
            if (args?.Exception != null && !stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("MQTT client disconnected: {Reason}", args.ReasonString ?? args.Exception.Message);
            }

            if (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // Pass HandleReconnectionAsync to execute within the reconnection lock
                    await _mqttClient.ReconnectAsync(stoppingToken, HandleReconnectionAsync);
                }
                catch (OperationCanceledException)
                {
                    // Shutting down
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Reconnection handler failed");
                }
            }
        });

        // Initial connection
        await _mqttClient.ConnectAsync(stoppingToken);

        // Publish initial birth certificates and state
        await _birthManager.DiscoverAndPublishAllNodeBirthsAsync(_sessionId, stoppingToken);
        await _dataRefreshService.PublishLatestStateAsync(stoppingToken);

        // Main poll loop
        var pollDelay = TimeSpan.FromMilliseconds(_settings.PollDelayMs);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await _outboxProcessor.ProcessMessagesAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing outbox");
            }

            // Periodic data refresh to prevent staleness
            if (_dataRefreshService.ShouldRefresh())
            {
                try
                {
                    await _dataRefreshService.RefreshDataAsync(stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error refreshing data");
                }
            }

            await Task.Delay(pollDelay, stoppingToken);
        }
    }

    private async Task HandleReconnectionAsync(CancellationToken ct)
    {
        var oldSessionId = _sessionId;
        _sessionId = Guid.NewGuid();
        _logger.LogInformation("Reconnected with new Sparkplug session ID: {SessionId}", _sessionId);

        await _repository.ClearBirthTrackingForSessionAsync(oldSessionId, ct);

        _sequenceManager.ClearAll();
        _birthManager.ClearBirthState();

        await _birthManager.DiscoverAndPublishAllNodeBirthsAsync(_sessionId, ct);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            if (_mqttClient.IsConnected)
            {
                var nodeIds = _birthManager.GetAllNodeIds();
                foreach (var nodeId in nodeIds)
                {
                    var groupId = _birthManager.GetNodeGroup(nodeId);
                    if (!string.IsNullOrEmpty(groupId))
                    {
                        await _birthManager.PublishNodeDeathCertificateAsync(groupId, nodeId, cancellationToken);
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
