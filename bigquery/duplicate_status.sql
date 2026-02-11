config {
  type:"assertion",
  name:"duplicate_status",
  dataset:"noora_health_task_whatsapp_chats",
  dependencyTargets:[{
    name:"ChatHist"
  }],
}
  -- this query returns messages and statuses if a message has the same status recorded more than once to catch inconsistencies in status records
SELECT message_id,status, count(*) as cnt
FROM noora_health_task_whatsapp_chats.ChatHist
GROUP BY message_id,status
HAVING cnt > 1
