CREATE OR REFRESH MATERIALIZED VIEW gold_complaints_chunked
COMMENT 'Complaint narratives chunked for RAG vector search. Filters empty narratives.'
AS
SELECT
  complaint_id,
  category,
  narrative,
  amount_lost,
  victim_city,
  modus_operandi,
  LENGTH(narrative) as narrative_length
FROM complaints
WHERE
  narrative IS NOT NULL 
  AND LENGTH(TRIM(narrative)) > 10
