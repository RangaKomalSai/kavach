CREATE OR REFRESH STREAMING TABLE silver_transactions (
  CONSTRAINT valid_txn_id EXPECT (txn_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW,
  CONSTRAINT not_self_transfer EXPECT (sender_upi_id != receiver_upi_id) ON VIOLATION DROP ROW
)
TBLPROPERTIES('pipelines.channel' = 'PREVIEW')
COMMENT 'Cleaned UPI transactions with derived features and AI-based risk classification'
AS
SELECT
  txn_id,
  created_at,
  sender_upi_id,
  receiver_upi_id,
  amount,
  txn_type,
  sender_bank,
  receiver_bank,
  receiver_account_age_days,
  is_scam_episode,
  scam_episode_id,
  -- Derived features
  date_format(created_at, 'HH') as hour_of_day,
  dayofweek(created_at) as day_of_week,
  CASE WHEN amount % 1000 = 0 OR amount % 5000 = 0 
       OR amount % 10000 = 0 OR amount % 25000 = 0 
       OR amount % 50000 = 0 OR amount % 100000 = 0 
       THEN true ELSE false END as is_round_amount,
  -- AI Gateway: Classify transaction risk using LLM
  CASE
    WHEN amount >= 25000
    THEN LOWER(TRIM(ai_query(
      'databricks-meta-llama-3-1-8b-instruct',
      CONCAT(
        'Classify this UPI transaction risk as exactly one of: low, medium, high. ',
        'Output ONLY the label. Transaction: ₹', CAST(amount AS STRING), 
        ' from ', sender_upi_id, ' to ', receiver_upi_id,
        ', receiver account age: ', CAST(receiver_account_age_days AS STRING), ' days.',
        ' Round amount: ', CASE WHEN amount % 10000 = 0 THEN 'yes' ELSE 'no' END
      )
    )))
    ELSE 'low'
  END AS ai_risk_label
FROM STREAM(transactions)
