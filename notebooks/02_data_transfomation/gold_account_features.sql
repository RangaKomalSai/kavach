CREATE OR REFRESH MATERIALIZED VIEW gold_account_features
COMMENT 'Account-level aggregated features for mule detection. One row per receiver account with transaction stats, round-amount ratios, and sender diversity metrics.'
AS
SELECT 
  receiver_upi_id as account_id,
  -- Volume features
  COUNT(*) as total_inbound_txns,
  SUM(amount) as total_inbound_volume,
  AVG(amount) as avg_inbound_amount,
  MAX(amount) as max_inbound_amount,
  -- Round amount signal
  AVG(CASE WHEN is_round_amount THEN 1.0 ELSE 0.0 END) as pct_round_amounts,
  -- Account age
  MIN(receiver_account_age_days) as account_age_at_first_inbound,
  -- Sender diversity
  COUNT(DISTINCT sender_upi_id) as unique_senders,
  -- Temporal spread
  COUNT(DISTINCT DATE(created_at)) as active_days,
  (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 3600 
      as activity_span_hours,
  -- Label
  MAX(CAST(is_scam_episode AS INT)) as is_mule
FROM silver_transactions
GROUP BY receiver_upi_id
