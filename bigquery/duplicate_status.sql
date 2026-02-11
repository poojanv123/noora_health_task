config {
  type:"assertion",
  name:"duplicate_status",
  dataset:"noora_health_task_whatsapp_chats",
  dependencyTargets:[{
    name:"ChatHist"
  }],
}
SELECT message_id,status, count(*) as cnt
FROM noora_health_task_whatsapp_chats.ChatHist
GROUP BY message_id,status
HAVING cnt > 1
