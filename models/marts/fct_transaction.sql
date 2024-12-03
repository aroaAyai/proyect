{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

WITH 
source AS (
    SELECT * 
    FROM {{ ref('stg_bank__account') }} 
),

cleaned AS (
    SELECT 
        account_id,
        account_status, 
        account_type,  
        date_opened,
        overdraft_limit, 
        balance, 
        last_activity,
        days_since_last_activity,
        balance_category,
        dateload
    FROM source
),

transactions AS (
    SELECT
        t.transaction_id,
        t.merchant_id,
        t.amount_usd,
        t.amount_eur,
        t.transaction_date,
        c.geo_id,
        c.country,  -- País
        c.customer_id,
        TO_CHAR(t.transaction_date, 'YYYYMMDD') AS time_key,
        m.merchant_category
    FROM {{ ref('stg_bank__transaction') }} t
    LEFT JOIN {{ ref('stg_google_sheets__customers') }} c
        ON t.customer_id = c.customer_id
    LEFT JOIN {{ ref('stg_bank__merchant') }} m
        ON t.merchant_id = m.merchant_id
)

SELECT 
    country,  -- Mostrar solo el país
    COUNT(transaction_id) AS transaction_count,
    ROUND(SUM(amount_usd), 2) AS total_amount_usd,
    ROUND(SUM(amount_eur), 2) AS total_amount_eur,
    ROUND(AVG(amount_usd), 2) AS avg_amount_usd,
    ROUND(AVG(amount_eur), 2) AS avg_amount_eur 
FROM transactions 
GROUP BY country  -- Agrupar solo por el país
ORDER BY total_amount_usd DESC