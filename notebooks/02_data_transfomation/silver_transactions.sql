CREATE OR REFRESH STREAMING TABLE silver_transactions (
  CONSTRAINT valid_txn_id EXPECT (txn_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW,
  CONSTRAINT not_self_transfer EXPECT (sender_upi_id != receiver_upi_id) ON VIOLATION DROP ROW
)
COMMENT 'Cleaned UPI transactions with derived features for fraud detection'
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
  CAST(date_format(created_at, 'HH') AS INT) as hour_of_day,
  dayofweek(created_at) as day_of_week,
  CASE 
    WHEN amount % 50000 = 0 OR amount % 25000 = 0 
      OR amount % 100000 = 0 OR amount % 10000 = 0
    THEN true ELSE false 
  END as is_round_amount
FROM STREAM(transactions)
WHERE
  sender_upi_id IS NOT NULL
  AND receiver_upi_id IS NOT NULL
