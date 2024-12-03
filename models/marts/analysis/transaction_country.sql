{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

WITH source AS (
    SELECT 
        t.transaction_id,
        t.merchant_id,
        t.amount_usd,
        t.amount_eur,
        t.transaction_date,
        c.customer_id,
        t.geo_id,
        c.country AS customer_country,
        m.merchant_category,
        d.device_id,
        d.device_type,
        d.valid_ip,
        d.geo_status,  
        g.country AS geo_country,
        g.city AS geo_city,
        g.is_proxy,
        t.time_key
    FROM {{ ref('fct_transaction') }} t
    LEFT JOIN {{ ref('dim_customer') }} c ON t.customer_id = c.customer_id
    LEFT JOIN {{ ref('dim_merchant') }} m ON t.merchant_id = m.merchant_id
    LEFT JOIN {{ ref('dim_device') }} d ON t.device_id = d.device_id  -- Corregido el JOIN aquí
    LEFT JOIN {{ ref('dim_geolocation') }} g ON t.geo_id = g.geo_id
    WHERE t.transaction_id IS NOT NULL
    {% if is_incremental() %}
        AND t.time_key > (SELECT MAX(time_key) FROM {{ this }})
    {% endif %}
),

stats_per_customer AS (
    SELECT 
        customer_id,
        AVG(amount_eur) AS avg_amount,
        STDDEV(amount_eur) AS stddev_amount,
        COUNT(*) AS transaction_count
    FROM {{ ref('fct_transaction') }}
    GROUP BY customer_id
),

amount_anomaly AS (
    SELECT 
        transaction_id,
        CASE
            WHEN amount_eur > spc.avg_amount + 3 * spc.stddev_amount THEN 'Sospechosa'
            ELSE 'Válida'
        END AS amount_status
    FROM source
    LEFT JOIN stats_per_customer spc
    ON source.customer_id = spc.customer_id
),

transaction_frequency AS (
    SELECT 
        transaction_id,
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
        CASE
            WHEN is_proxy = TRUE THEN 'Sospechosa'
            ELSE 'Válida'
        END AS proxy_status
    FROM source
),

combined_status AS (
    SELECT 
        t.transaction_id,
        amount_anomaly.amount_status,
        transaction_frequency.frequency_status,
        proxy_detection.proxy_status,
        (CASE WHEN amount_anomaly.amount_status = 'Sospechosa' THEN 1 ELSE 0 END
          +
          CASE WHEN transaction_frequency.frequency_status = 'Sospechosa' THEN 1 ELSE 0 END
          +
          CASE WHEN proxy_detection.proxy_status = 'Sospechosa' THEN 1 ELSE 0 END) AS suspect_count,
        time_key,
        g.geo_country  -- Añadimos el país de la geolocalización
    FROM source t
    LEFT JOIN amount_anomaly ON t.transaction_id = amount_anomaly.transaction_id
    LEFT JOIN transaction_frequency ON t.transaction_id = transaction_frequency.transaction_id
    LEFT JOIN proxy_detection ON t.transaction_id = proxy_detection.transaction_id
    LEFT JOIN {{ ref('dim_geolocation') }} g ON t.geo_id = g.geo_id  -- Asegúrate de que geo_country esté en el JOIN
),

final_status_classification AS (
    SELECT 
        transaction_id,
        amount_status,
        frequency_status,
        proxy_status,
        geo_country,  -- País de geolocalización
        CASE
            WHEN suspect_count = 1 THEN 'Sospecha'
            WHEN suspect_count >= 2 THEN 'Fraude'
            ELSE 'Normal'
        END AS final_status,
        time_key
    FROM combined_status
)

-- Análisis por país
SELECT 
    geo_country,  -- País de geolocalización
    COUNT(DISTINCT transaction_id) AS total_transactions,
    COUNT(DISTINCT CASE WHEN final_status = 'Sospecha' THEN transaction_id END) AS suspect_transactions,
    COUNT(DISTINCT CASE WHEN final_status = 'Fraude' THEN transaction_id END) AS fraud_transactions,
    COUNT(DISTINCT CASE WHEN final_status = 'Normal' THEN transaction_id END) AS normal_transactions,
    AVG(amount_eur) AS avg_amount_per_transaction,  -- Promedio de cantidad por transacción
    STDDEV(amount_eur) AS stddev_amount_per_transaction  -- Desviación estándar del monto por transacción
FROM final_status_classification
GROUP BY geo_country
ORDER BY total_transactions DESC
