-- tests/test_last_activity_after_opened.sql

WITH validation AS (
    SELECT
        account_id,
        date_opened,
        last_activity,
        CASE
            WHEN last_activity <= date_opened THEN 1
            ELSE 0
        END AS invalid_rows
    FROM {{ ref('base_account') }}  
)

SELECT
    COUNT(*) AS num_invalid_rows
FROM validation
WHERE invalid_rows = 1
