{{ config(
    materialized='table',  
) }}

WITH customer_transactions AS (
    SELECT
        c.CUSTOMER_ID,
        a.ACCOUNT_TYPE,
        a.ACCOUNT_STATUS,
        COUNT(t.TRANSACTION_ID) AS transaction_count
    FROM {{ ref('fct_transaction') }} t
    LEFT JOIN {{ ref('dim_customer') }} c 
        ON t.CUSTOMER_ID = c.CUSTOMER_ID
    LEFT JOIN {{ ref('dim_account') }} a
        ON c.CUSTOMER_ID = a.ACCOUNT_ID 
    {% if is_incremental() %}
    WHERE t.TRANSACTION_DATE > (SELECT MAX(transaction_date) FROM {{ this }})
    {% endif %}
    GROUP BY c.CUSTOMER_ID, a.ACCOUNT_TYPE, a.ACCOUNT_STATUS
),

account_status_type_summary AS (
    SELECT
        ACCOUNT_STATUS,
        ACCOUNT_TYPE,
        SUM(transaction_count) AS total_transactions
    FROM customer_transactions
    GROUP BY ACCOUNT_STATUS, ACCOUNT_TYPE
)

SELECT 
    ACCOUNT_STATUS,
    ACCOUNT_TYPE,
    total_transactions,
FROM account_status_type_summary
ORDER BY total_transactions DESC