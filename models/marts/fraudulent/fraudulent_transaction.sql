{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

WITH source AS (
    -- Garantizar registros únicos en el origen usando ROW_NUMBER
    SELECT *
    FROM (
        SELECT 
            t.transaction_id,
            t.merchant_id,
            t.amount_usd,
            t.amount_eur,
            t.transaction_date,
            c.customer_id,
            d.geo_id,
            c.country AS customer_country,
            m.merchant_category,
            d.device_id,
            d.device_type,
            d.valid_ip,
            d.geo_status,  
            g.country AS geo_country,
            g.city AS geo_city,
            g.is_proxy,
            t.time_key,
            ROW_NUMBER() OVER (PARTITION BY t.transaction_id ORDER BY t.transaction_date DESC) AS rn
        FROM {{ ref('fct_transaction') }} t
        LEFT JOIN {{ ref('dim_customer') }} c ON t.customer_id = c.customer_id
        LEFT JOIN {{ ref('dim_merchant') }} m ON t.merchant_id = m.merchant_id
        LEFT JOIN {{ ref('dim_device') }} d ON t.customer_id = d.customer_id
        LEFT JOIN {{ ref('dim_geolocation') }} g ON t.geo_id = g.geo_id
        WHERE t.transaction_id IS NOT NULL
    )
    WHERE rn = 1 -- Seleccionamos solo un registro por transaction_id
),

-- Asegurar que cada subconsulta tenga registros únicos
geo_discrepancy AS (
    SELECT 
        transaction_id,
        customer_id,
        geo_country,
        customer_country,
        CASE
            WHEN geo_country != customer_country THEN 'Sospechosa'
            ELSE 'Válida'
        END AS geo_status
    FROM source
),

device_suspicion AS (
    SELECT 
        transaction_id,
        customer_id,
        device_id,
        device_type,
        valid_ip,
        geo_status AS device_status
    FROM source
),

amount_anomaly AS (
    SELECT 
        transaction_id,
        customer_id,
        amount_usd,
        amount_eur,
        CASE
            WHEN amount_usd > (
                SELECT AVG(amount_usd) + 3 * STDDEV(amount_usd) 
                FROM {{ ref('fct_transaction') }} 
                WHERE customer_id = source.customer_id
            ) THEN 'Sospechosa'
            ELSE 'Válida'
        END AS amount_status
    FROM source
),

transaction_frequency AS (
    SELECT 
        transaction_id,
        customer_id,
        merchant_id,
        COUNT(*) OVER (PARTITION BY customer_id) AS customer_transaction_count,
        COUNT(*) OVER (PARTITION BY merchant_id) AS merchant_transaction_count,
        CASE 
            WHEN COUNT(*) OVER (PARTITION BY customer_id) > 10 THEN 'Sospechosa'
            WHEN COUNT(*) OVER (PARTITION BY merchant_id) > 20 THEN 'Sospechosa'
            ELSE 'Válida'
        END AS frequency_status
    FROM source
),

proxy_detection AS (
    SELECT 
        transaction_id,
        customer_id,
        device_id,
        geo_id,
        is_proxy,
        CASE
            WHEN is_proxy = TRUE THEN 'Sospechosa'
            ELSE 'Válida'
        END AS proxy_status
    FROM source
),

-- Combinar todas las reglas
fraudulent_transactions AS (
    SELECT 
        t.transaction_id,
        t.customer_id,
        t.merchant_id,
        t.amount_usd,
        t.amount_eur,
        t.transaction_date,
        t.geo_id,
        t.customer_country,
        t.geo_country,
        t.device_id,
        t.device_type,
        t.valid_ip,
        t.geo_city,
        t.is_proxy,
        t.time_key,
        geo_discrepancy.geo_status AS geo_status,
        device_suspicion.device_status AS device_status,
        amount_anomaly.amount_status AS amount_status,
        transaction_frequency.frequency_status AS frequency_status,
        proxy_detection.proxy_status AS proxy_status,
        CASE
            WHEN geo_discrepancy.geo_status = 'Sospechosa'
                 OR device_suspicion.device_status = 'Sospechosa'
                 OR amount_anomaly.amount_status = 'Sospechosa'
                 OR transaction_frequency.frequency_status = 'Sospechosa'
                 OR proxy_detection.proxy_status = 'Sospechosa' THEN 'Fraudulenta'
            ELSE 'Válida'
        END AS final_status,
        ROW_NUMBER() OVER (PARTITION BY t.transaction_id ORDER BY t.transaction_date DESC) AS rn
    FROM source t
    LEFT JOIN geo_discrepancy ON t.transaction_id = geo_discrepancy.transaction_id
    LEFT JOIN device_suspicion ON t.transaction_id = device_suspicion.transaction_id
    LEFT JOIN amount_anomaly ON t.transaction_id = amount_anomaly.transaction_id
    LEFT JOIN transaction_frequency ON t.transaction_id = transaction_frequency.transaction_id
    LEFT JOIN proxy_detection ON t.transaction_id = proxy_detection.transaction_id
)

-- Seleccionar resultados únicos al final
SELECT 
    transaction_id,
    customer_id,
    merchant_id,
    amount_usd,
    amount_eur,
    transaction_date,
    geo_id,
    customer_country,
    geo_country,
    geo_city,
    device_id,
    device_type,
    valid_ip,
    is_proxy,
    geo_status,
    device_status,
    amount_status,
    frequency_status,
    proxy_status,
    final_status,
    time_key
FROM fraudulent_transactions
WHERE rn = 1 

{% if is_incremental() %}
    AND time_key > (SELECT MAX(time_key) FROM {{ this }})
{% endif %}
