{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

SELECT
    t.transaction_id,
    t.merchant_id,
    t.amount_usd,
    t.amount_eur,
    t.transaction_date,
    c.geo_id,
    c.customer_id,
    TO_CHAR(t.transaction_date, 'YYYYMMDD') AS time_key,
    m.merchant_category
FROM {{ ref('stg_bank__transaction') }} t
LEFT JOIN {{ ref('stg_google_sheets__customers') }} c
    ON t.customer_id = c.customer_id
LEFT JOIN {{ ref('stg_bank__merchant') }} m
    ON t.merchant_id = m.merchant_id


{% if is_incremental() %}
    WHERE time_key > (SELECT MAX(time_key) FROM {{ this }}) 
{% endif %}
