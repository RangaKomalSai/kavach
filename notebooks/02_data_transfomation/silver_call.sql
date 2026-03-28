CREATE OR REFRESH STREAMING TABLE silver_calls (
  CONSTRAINT valid_call_id EXPECT (call_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_duration EXPECT (duration_seconds > 0) ON VIOLATION DROP ROW
)
COMMENT 'Cleaned call metadata for call-transaction correlation analysis'
AS
SELECT
  call_id,
  created_at,
  caller_number,
  callee_number,
  duration_seconds,
  call_type,
  is_international
FROM STREAM(calls)
WHERE
  caller_number IS NOT NULL
  AND callee_number IS NOT NULL
