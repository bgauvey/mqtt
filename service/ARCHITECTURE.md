# MQTT Service Architecture

## Overview
This service has been refactored from a 1200+ line monolithic class into a clean, maintainable architecture with proper separation of concerns.

## Project Structure

```
service/
├── Configuration/          # Configuration classes
│   └── MqttSettings.cs    # MQTT settings model
├── Models/                # Data transfer objects
│   ├── OutboxMessage.cs   # Outbox message model
│   └── TopicInfo.cs       # Parsed topic information
├── Repositories/          # Data access layer
│   ├── IOutboxRepository.cs
│   └── OutboxRepository.cs
├── Services/              # Business logic layer
│   ├── BirthCertificateManager.cs  # Handles NBIRTH/DBIRTH certificates
│   ├── DataRefreshService.cs       # Periodic data refresh to prevent staleness
│   ├── MqttClientManager.cs        # MQTT connection lifecycle
│   ├── OutboxProcessorService.cs   # Processes outbox messages
│   ├── SequenceManager.cs          # Sparkplug sequence number management
│   └── SparkplugService.cs         # Sparkplug B protocol logic
└── Program.cs             # Application entry point and orchestration
```

## Layer Responsibilities

### Configuration Layer
- **MqttSettings**: Holds all MQTT-related configuration including broker, port, credentials, poll delay, and data refresh interval

### Models Layer
- **OutboxMessage**: Represents a message from the database outbox
- **TopicInfo**: Parsed Sparkplug B topic with GroupId, NodeId, and DeviceName

### Repository Layer
- **IOutboxRepository**: Interface for database operations
- **OutboxRepository**: SQL Server data access implementation
  - Fetches unprocessed messages
  - Gets latest messages per topic
  - Manages birth tracking table
  - Marks messages as processed

### Services Layer

#### SparkplugService
- Parses Sparkplug B topics
- Converts JSON to Sparkplug metrics
- Serializes payloads to Sparkplug B protobuf format
- No dependencies on other services (pure business logic)

#### SequenceManager
- Thread-safe sequence number management per node
- Ensures proper Sparkplug B sequence ordering
- Supports reset and clearing for reconnection scenarios

#### MqttClientManager
- MQTT connection lifecycle management
- Reconnection with exponential backoff
- Last Will and Testament (LWT) configuration
- Publishing messages

#### BirthCertificateManager
- Discovers nodes and devices from the database
- Publishes NBIRTH (Node Birth) certificates
- Publishes DBIRTH (Device Birth) certificates
- Publishes NDEATH (Node Death) certificates on shutdown
- Ensures proper Sparkplug B birth ordering (NBIRTH → DBIRTH → DDATA)
- Tracks birthed devices per session

#### DataRefreshService
- **Key Feature**: Prevents data staleness in Ignition
- Periodically republishes latest values for all topics
- Configurable refresh interval (default: 30 seconds)
- Ensures tags remain "good" quality in Ignition

#### OutboxProcessorService
- Polls database for unprocessed messages
- Ensures nodes are birthed before publishing data
- Converts and publishes messages
- Marks messages as processed
- Handles SQL retry logic

### Application Layer
- **Program.cs**:
  - Dependency injection configuration
  - Service registration
  - Main execution loop orchestration
  - Graceful shutdown handling

## Key Features

### 1. Data Staleness Prevention
The `DataRefreshService` automatically republishes the latest data every 30 seconds (configurable) to prevent Ignition tags from being marked as "stale" or "bad quality."

### 2. Sparkplug B Compliance
- Proper birth certificate ordering (NBIRTH → DBIRTH → DDATA)
- Sequence number management per node
- Last Will and Testament (NDEATH) for ungraceful disconnections
- Session-based tracking of births

### 3. Resilience
- Automatic reconnection with exponential backoff
- Session management for reconnection scenarios
- SQL retry logic
- Thread-safe state management

### 4. Scalability
- Service-based architecture allows easy testing and mocking
- Clear separation of concerns
- Dependency injection for loose coupling
- Repository pattern for data access abstraction

## Configuration

Configure the service via `appsettings.json`:

```json
{
  "ConnectionStrings": {
    "SqlServer": "Server=localhost;Database=Factelligence;..."
  },
  "Mqtt": {
    "Broker": "localhost",
    "Port": 1883,
    "Username": "admin",
    "Password": "admin",
    "PollDelayMs": 500,
    "DataRefreshIntervalSeconds": 30
  }
}
```

### Settings Explained
- **PollDelayMs**: Delay between outbox polling cycles (default: 500ms)
- **DataRefreshIntervalSeconds**: How often to republish data to prevent staleness (default: 30s)

## Execution Flow

1. **Startup**
   - Initialize MQTT client with LWT
   - Connect to MQTT broker
   - Discover nodes and devices from database
   - Publish NBIRTH and DBIRTH certificates
   - Publish latest state for all topics

2. **Main Loop**
   - Process unprocessed outbox messages
   - Ensure nodes are birthed before publishing
   - Publish DDATA messages
   - Mark messages as processed
   - Periodically refresh data (every 30s)

3. **Reconnection**
   - Create new session ID
   - Clear old birth tracking
   - Reset sequences
   - Republish all birth certificates
   - Continue processing

4. **Shutdown**
   - Publish NDEATH for all active nodes
   - Disconnect from MQTT broker gracefully

## Testing Strategy

Each service can be tested independently:
- **SparkplugService**: Pure functions, easy to unit test
- **SequenceManager**: Thread-safety tests
- **MqttClientManager**: Mock MQTT client
- **Repository**: Mock SQL connections or use in-memory database
- **Services**: Mock their dependencies

## Benefits of Refactoring

1. **Maintainability**: Each class has a single, clear responsibility
2. **Testability**: Services can be tested in isolation
3. **Readability**: ~150 lines per file vs 1200+ in one file
4. **Extensibility**: Easy to add new features or modify existing ones
5. **Debugging**: Easier to locate and fix issues
6. **Team Collaboration**: Multiple developers can work on different services

## Future Enhancements

- Add health checks for monitoring
- Implement metrics/telemetry
- Add configuration validation
- Create integration tests
- Add circuit breaker pattern for SQL failures
- Support for multiple MQTT brokers (high availability)
