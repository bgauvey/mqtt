CREATE TABLE dbo.MQTTOutbox (
    Id BIGINT IDENTITY(1,1) PRIMARY KEY,
    Topic NVARCHAR(255) NOT NULL,
    Payload NVARCHAR(MAX) NOT NULL,
    CreatedAt DATETIME2 DEFAULT GETUTCDATE(),
    ProcessedAt DATETIME2 NULL,
    IsProcessed BIT DEFAULT 0
);

CREATE INDEX IX_MQTTOutbox_Unprocessed 
ON dbo.MQTTOutbox (IsProcessed, CreatedAt) 
WHERE IsProcessed = 0;