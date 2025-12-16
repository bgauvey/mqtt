CREATE OR ALTER TRIGGER trg_job_mqtt_statechanged
ON dbo.job
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Only fire when state_cd changes
    IF UPDATE(state_cd)
    BEGIN
        INSERT INTO dbo.MQTTOutbox (Topic, Payload)
        SELECT 
            'mes/jobs/status/' + CAST(i.wo_id AS NVARCHAR(50)),
            (
                SELECT 
                    i.wo_id AS orderNumber,
                    i.state_cd AS statusId,
                    s.state_desc AS statusName,
                    GETUTCDATE() AS timestamp
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            )
        FROM inserted i
        INNER JOIN deleted d ON i.wo_id = d.wo_id AND i.oper_id = d.oper_id AND i.seq_no = d.seq_no
        LEFT JOIN dbo.job_state s ON i.state_cd = s.state_cd
        WHERE i.state_cd <> d.state_cd;
    END
END;