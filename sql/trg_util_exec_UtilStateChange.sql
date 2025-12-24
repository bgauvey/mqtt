SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE OR ALTER   TRIGGER [dbo].[trg_util_exec_UtilStateChange]
ON [dbo].[util_exec]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    -- Only fire when cur_reas_cd changes
    IF UPDATE(cur_reas_cd)
    BEGIN
    --spBv1.0/[Group ID]/[Message Type]/[EON Node ID]/[Device ID]
        INSERT INTO dbo.MQTTOutbox (Topic, Payload)
        SELECT 
            'spBv1.0/BUL/DDATA/UtilExecBridge/' + e.ent_name AS Topic,
            (
                SELECT 
                    COALESCE(i.cur_reas_cd, 0) AS reas_cd,
                    COALESCE(i.reas_reqd, CAST(0 AS BIT)) AS reasReqd,
                    COALESCE(ur.reas_desc, '') AS reasDesc,
                    COALESCE(us.state_desc, '') AS stateDesc,
                    COALESCE(us.color, 255) AS color,
                    COALESCE(i.cur_raw_reas_cd, '') AS utilRawReasCd,
                    GETUTCDATE() AS timestamp
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            )
        FROM inserted i
        INNER JOIN deleted d ON i.ent_id = d.ent_id
        LEFT JOIN dbo.util_reas ur ON i.cur_reas_cd = ur.reas_cd
        LEFT JOIN util_state us ON i.cur_state_cd = us.state_cd
        INNER JOIN dbo.ent e ON i.ent_id = e.ent_id
        WHERE COALESCE(i.cur_reas_cd, 0) <> COALESCE(d.cur_reas_cd, 0);
    END
END;
GO
ALTER TABLE [dbo].[util_exec] ENABLE TRIGGER [trg_util_exec_UtilStateChange]
GO
