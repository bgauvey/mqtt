`
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐     ┌──────────────┐
│   SQL Server    │     │  Outbox Table    │     │  .NET Service    │     │ MQTT Broker  │
│                 │     │                  │     │                  │     │  (Mosquitto) │
│ Orders Table    │────►│ MQTTOutbox       │────►│ Polls & Publishes│────►│              │
│ (UPDATE Trigger)│     │ - Topic          │     │                  │     │              │
│                 │     │ - Payload        │     │                  │     │              │
└─────────────────┘     │ - CreatedAt      │     └──────────────────┘     └──────────────┘
                        │ - Processed      │                                    │
                        └──────────────────┘                                    ▼
                                                                         ┌──────────────┐
                                                                         │  Ignition    │
                                                                         │  Consumers   │
                                                                         └──────────────┘
`

 ## Summary
| Component | Technology | Purpose |
| --------- | ---------- | ------- |
| Change Detection | SQL Trigger | Captures status changes |
| Message | QueueOutbox Table | Reliable, transactional buffer |
| Publisher | .NET Worker Service | Polls outbox, publishes to MQTT |
| Broker | Mosquitto | Routes messages to subscribers | 
| Consumer | Ignition MQTT Engine | Receives and processes events

This pattern is called the Transactional Outbox Pattern — it ensures messages aren't lost even if the MQTT broker is temporarily unavailable.                                                                     