config {
  type:"assertion",
  name:"check_datetime_format",
  dataset:"noora_health_task_whatsapp_chats",
  dependencyTargets:[{
    name:"ChatHist"
  }],
}
select *
FROM `noora_health_task_whatsapp_chats.ChatHist`
where messages_inserted_at is null
