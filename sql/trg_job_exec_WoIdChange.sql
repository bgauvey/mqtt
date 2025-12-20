SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE OR ALTER   TRIGGER [dbo].[trg_job_exec_WoIdChange]
ON [dbo].[job_exec]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Only fire when state_cd changes
    IF UPDATE(cur_wo_id)
    BEGIN
    --spBv1.0/[Group ID]/[Message Type]/[EON Node ID]/[Device ID]
        INSERT INTO dbo.MQTTOutbox (Topic, Payload)
        SELECT 
            'spBv1.0/BUL/DDATA/JobExecBridge/' + e.ent_name AS Topic,
            (
                SELECT 
                    COALESCE(i.cur_wo_id, '') AS woId,
                    COALESCE(i.cur_oper_id, '') AS operId,
                    COALESCE(i.cur_seq_no, 0) AS seqNo,
                    COALESCE(item.item_id, '') AS itemId,
                    GETUTCDATE() AS timestamp
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            )
        FROM inserted i
        INNER JOIN deleted d ON i.ent_id = d.ent_id
        LEFT JOIN dbo.job j ON i.cur_wo_id = j.wo_id AND i.cur_oper_id = j.oper_id AND i.cur_seq_no = j.seq_no
        LEFT JOIN item item ON j.item_id = item.item_id
        INNER JOIN ent e ON i.ent_id = e.ent_id
        WHERE COALESCE(i.cur_wo_id, '') <> COALESCE( d.cur_wo_id, '');
    END
END;
GO
ALTER TABLE [dbo].[job_exec] ENABLE TRIGGER [trg_job_exec_WoIdChange]
GO
