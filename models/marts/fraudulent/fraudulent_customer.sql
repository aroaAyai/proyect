{{ config(
    materialized='table',
) }}

WITH source AS (
    SELECT 
        t.customer_id,
        t.transaction_id,
        t.amount_eur,
        t.transaction_date,
        c.name,
        c.country AS customer_country,
        c.registration_date,
        c.phone_validation_status,
        g.country AS geo_country,
        g.is_proxy,
        c.age_years
    FROM {{ ref('fct_transaction') }} t
    LEFT JOIN {{ ref('dim_customer') }} c ON t.customer_id = c.customer_id
    LEFT JOIN {{ ref('dim_geolocation') }} g ON t.geo_id = g.geo_id
    WHERE t.transaction_id IS NOT NULL

),

transaction_stats AS (
    SELECT 
        customer_id,
        AVG(amount_eur) AS avg_transaction_amount,
        STDDEV(amount_eur) AS stddev_transaction_amount,
        COUNT(*) AS transaction_count
    FROM {{ ref('fct_transaction') }}
    GROUP BY customer_id
),

customer_risk_indicators AS (
    SELECT 
        s.customer_id,
        s.name,
        s.customer_country,
        s.registration_date,
        t.transaction_count,
        CASE 
            WHEN t.transaction_count > 2 THEN 'Sospechosa' 
            ELSE 'Válida'
        END AS transaction_volume_status,
        CASE 
            WHEN s.customer_country != s.geo_country THEN 'Sospechosa'
            ELSE 'Válida'
        END AS geo_mismatch_status,
        CASE 
            WHEN s.phone_validation_status = 'INVALID' THEN 'Sospechosa' 
            ELSE 'Válida'
        END AS phone_validation_status
    FROM source s
    LEFT JOIN transaction_stats t ON s.customer_id = t.customer_id
),

customer_risk_classification AS (
    SELECT 
        customer_id,
        name,
        transaction_volume_status,
        geo_mismatch_status,
        phone_validation_status,
        (CASE WHEN transaction_volume_status = 'Sospechosa' THEN 1 ELSE 0 END +
         CASE WHEN geo_mismatch_status = 'Sospechosa' THEN 1 ELSE 0 END +
         CASE WHEN phone_validation_status = 'Sospechosa' THEN 1 ELSE 0 END) AS total_risk,
        CASE 
            WHEN total_risk = 1 THEN 'Sospecha leve'
            WHEN total_risk = 2 THEN 'Sospecha moderada'
            WHEN total_risk >= 3 THEN 'Fraude'
            ELSE 'Normal'
        END AS risk_classification
    FROM customer_risk_indicators
)

SELECT 
    customer_id,
    name,
    transaction_volume_status,
    geo_mismatch_status,
    phone_validation_status,
    total_risk,
    risk_classification
FROM customer_risk_classification
ORDER BY total_risk asc