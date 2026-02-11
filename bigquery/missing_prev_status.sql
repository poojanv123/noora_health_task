config {
  type:"assertion",
  name:"missing_prev_status",
  dataset:"noora_health_task_whatsapp_chats",
  dependencyTargets:[{
    name:"ChatHist"
  }],
}
WITH ordered AS (
  SELECT
    message_uuid,
    status,
    statuses_inserted_at,
    LAG(status) OVER (
      PARTITION BY message_uuid
      ORDER BY statuses_inserted_at
    ) AS prev_status
  FROM noora_health_task_whatsapp_chats.ChatHist
)
SELECT *
FROM ordered
WHERE
  (status = 'delivered' AND prev_status NOT IN ('sent'))
  OR (status = 'read' AND prev_status NOT IN ('delivered'))
