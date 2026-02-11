config {
  type:"assertion",
  name:"wrong_order",
  dataset:"noora_health_task_whatsapp_chats",
  dependencyTargets:[{
    name:"ChatHist"
  }],
}
-- this query returns records where the order of status is incorrect. the query calculates the previous status for each row and checks where the previous status aligns with the correct order of statuses i.e. sent before delivered and delivered before read.
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
  (status = 'delivered' AND prev_status ='read')
  OR (status = 'sent' AND prev_status = 'delivered') OR 
  (status = 'sent' AND prev_status = 'read')
