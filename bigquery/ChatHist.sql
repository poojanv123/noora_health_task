config {
  type:"table",
  name:"ChatHist",
  dataset:"noora_health_task_whatsapp_chats",
}
WITH chathistory AS (
SELECT 
  messages.* EXCEPT (_airbyte_raw_id, _airbyte_extracted_at, _airbyte_meta,_airbyte_generation_id,id, uuid,inserted_at,updated_at),
  statuses.* EXCEPT (_airbyte_raw_id, _airbyte_extracted_at, _airbyte_meta,_airbyte_generation_id,id,uuid,inserted_at,updated_at,message_id,message_uuid),
  statuses.message_id as statuses_message_id,
  statuses.message_uuid as statuses_message_uuid,
  messages.id as message_id,
  messages.uuid as message_uuid,
  statuses.id as status_id,
  statuses.uuid as status_uuid,
  SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', statuses.inserted_at) AS statuses_inserted_at,
  SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', statuses.updated_at) AS statuses_updated_at,
  SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', messages.inserted_at) AS messages_inserted_at,
  SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', messages.updated_at) AS messages_updated_at
 FROM `noora_health_task_whatsapp_chats.Messages` AS messages
LEFT JOIN `noora_health_task_whatsapp_chats.Statuses` AS statuses
  ON messages.id = statuses.message_id),
chathist_with_time_diff as(
SELECT
  chathistory.*,
  MAX(status= 'failed')
    OVER (PARTITION BY message_id) AS has_failed,
  TIMESTAMP_DIFF(
    MAX(IF(status = 'read', statuses_inserted_at, NULL)) 
      OVER (PARTITION BY message_id),
    MAX(IF(status = 'sent', statuses_inserted_at, NULL)) 
      OVER (PARTITION BY message_id),
    HOUR
  ) AS diff_sent_to_read
FROM chathistory)
SELECT
  chathist_with_time_diff.* EXCEPT(diff_sent_to_read),
  CASE
    WHEN status = 'read' THEN diff_sent_to_read
    ELSE NULL
  END AS hr_to_read_mins
FROM chathist_with_time_diff
