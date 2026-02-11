config {
  type:"assertion",
  name:"check_datetime_format",
  dataset:"noora_health_task_whatsapp_chats",
  dependencyTargets:[{
    name:"ChatHist"
  }],
}
-- this query returns rows with messages_inserted_at value as null to get rows that failed timestamp parse due to incompatible string structure
select *
FROM `noora_health_task_whatsapp_chats.ChatHist`
where messages_inserted_at is null
