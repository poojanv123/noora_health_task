config {
  type:"assertion",
  name:"missing_status",
  dataset:"noora_health_task_whatsapp_chats",
  dependencyTargets:[{
    name:"ChatHist"
  }],
}
  -- this query returns messages for which statuses data is missing
select count(*) as cnt
FROM `noora_health_task_whatsapp_chats.ChatHist`
where status_id is null
