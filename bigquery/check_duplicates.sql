config {
  type:"assertion",
  name:"check_duplicates",
  dataset:"noora_health_task_whatsapp_chats",
  dependencyTargets:[{
    name:"ChatHist"
  }]
}
-- this query joins the data table with itself on content to get rows with identical content and filter for difference in inserted_at time less than 2 minutes to get near duplicate records
SELECT
  a.message_id AS message_id_a,
  b.message_id AS message_id_b,
  a.content,
  a.messages_inserted_at AS ts_a,
  b.messages_inserted_at AS ts_b,
  ABS(TIMESTAMP_DIFF(a.messages_inserted_at, b.messages_inserted_at, SECOND)) AS diff_seconds
FROM `noora_health_task_whatsapp_chats.ChatHist` a
JOIN `noora_health_task_whatsapp_chats.ChatHist` b
  ON a.content = b.content
 where a.message_id != b.message_id
 AND ABS(TIMESTAMP_DIFF(a.messages_inserted_at, b.messages_inserted_at, SECOND)) < 120
