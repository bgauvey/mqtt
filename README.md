# MqttBridgeService

⚡ A small .NET Worker service that reads JSON messages from a SQL-based outbox and publishes them as Sparkplug B messages to an MQTT broker.

---

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Database](#database)
- [Running locally](#running-locally)
- [Testing](#testing)
- [Troubleshooting & Known issues](#troubleshooting--known-issues)
- [Contributing](#contributing)
- [License](#license)

---

## Overview
MqttBridgeService monitors a SQL outbox table (`dbo.MQTTOutbox`) for JSON payloads that represent Sparkplug B metrics and publishes them to an MQTT broker as Sparkplug B payloads. It also handles Sparkplug NBIRTH/DBIRTH/NDEATH certificates and periodic refreshes to prevent staleness.

## Features
- Reads and publishes messages from a SQL outbox
- Converts JSON payloads into Sparkplug B `Proto` payloads
- Publishes NBIRTH/DBIRTH/NDEATH certificates and refreshes topic state
- Supports Last Will and Testament (LWT) configuration based on discovered nodes
- Minimal dependency list, written as a .NET Worker

## Architecture
- `MqttPublisherService` (hosted BackgroundService) - main orchestrator
- `OutboxRepository` - database access for outbox & birth tracking
- `OutboxProcessorService` - converts and publishes outbox messages
- `BirthCertificateManager` - publishes NBIRTH/DBIRTH/NDEATH certificates
- `SparkplugService` - topic parsing and Sparkplug payload creation
- `MqttClientManager` - single client, reconnect/backoff logic
- `SequenceManager` - per-node sequence numbers for Sparkplug

## Prerequisites
- .NET 8 SDK
- SQL Server (or compatible) with the `MQTTOutbox` table in `dbo` schema
- An MQTT Broker (e.g., mosquitto)

## Quick start
1. Clone the repo and open the `service` folder:

```bash
git clone <repo>
cd service
```

2. Configure connection strings and MQTT settings (see [Configuration](#configuration)).
3. Build and run the service:

```bash
dotnet build
dotnet run --project ./MqttBridgeService.csproj
```

## Configuration
Configuration uses `appsettings.json` in `service/` and the `MqttSettings` POCO:

- Connection strings: `ConnectionStrings:SqlServer`
- MQTT settings (section `Mqtt`): `Broker`, `Port`, `Username`, `Password`, `PollDelayMs`, `DataRefreshIntervalSeconds`

Sensitive values (such as `Mqtt:Password`) should be provided via environment variables or secret store in production (User Secrets / Key Vault).

Example minimal `appsettings.json` snippet:

```json
{
  "ConnectionStrings": {
    "SqlServer": "Server=.;Database=MyDb;Trusted_Connection=True;"
  },
  "Mqtt": {
    "Broker": "localhost",
    "Port": 1883,
    "Username": "",
    "Password": "",
    "PollDelayMs": 500,
    "DataRefreshIntervalSeconds": 30
  }
}
```

## Database
- The repo contains `sql/mqtt_outbox.sql` for the expected schema of the outbox table.
- The `OutboxRepository` will create `SparkplugBirthTracking` table automatically on first run if it does not exist.

Make sure the service account/connection string has the necessary CREATE/ALTER rights if you rely on that behavior in non-production environments.

## Running locally
- Start your SQL Server and MQTT broker.
- Seed the `MQTTOutbox` table with sample `Topic` and `Payload` rows. Topics are expected in the Sparkplug format (e.g., `spBv1.0/{GroupId}/DDATA/{NodeId}/{Device}`) and payloads should be JSON objects mapping metric names to values.
- Run the app as described above.

## Testing
- Unit tests are in `MqttBridgeService.Tests`. Run them with:

```bash
dotnet test ./MqttBridgeService.Tests
```

## Troubleshooting & Known issues ⚠️
I reviewed the code and found a few issues to be aware of:

- **Potential NullReferenceException in `GetGroupIdForNodeAsync`** (BirthCertificateManager): the code accesses `.Topic` on a `FirstOrDefault` result without null checking. Update will be required to make this safe.
- **Blocking `SemaphoreSlim.Wait()` calls** in `SequenceManager` and `BirthCertificateManager` can cause threadpool blocking under load; prefer `WaitAsync` or `ConcurrentDictionary` patterns for async paths.
- **Numeric conversion edge cases** in `SparkplugService` (e.g., converting integers to unsigned) may throw or misrepresent values — validate/handle conversions.
- The current **topic parsing** focuses on `DDATA` only; if you rely on other Sparkplug topics in code paths, expand parsing accordingly.

If you hit any runtime exception, enable debug logs and check the topic and payload shape for malformed or unexpected payloads.

## Contributing
- Please open issues or PRs for bug fixes or feature requests.
- Add unit tests for any behavior you change.
- Run analyzers and linters before opening PRs. Adding `Microsoft.CodeAnalysis.NetAnalyzers` and a CI job to run `dotnet build` and `dotnet test` is recommended.

### Suggested first PRs
- Fix the `GetGroupIdForNodeAsync` NRE.
- Convert Semaphore usage to async-friendly APIs and add concurrency tests for `SequenceManager`.
- Add analyzers and a GitHub Actions workflow to run build/tests.

## License
This repository does not include a license file. If you plan to publish, add a `LICENSE` file (e.g., MIT) to clarify the project license.

---

If you'd like, I can add a CI workflow, a CONTRIBUTING.md, or implement the high-priority fixes (NRE + Semaphore changes and tests).