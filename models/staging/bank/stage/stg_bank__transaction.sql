{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

WITH source AS (
    SELECT * 
    FROM {{ source('bank', 'transaction') }}
),

renamed AS (
    SELECT
        transaction_id,
        account_id, 
        merchant_id, 
        device_id,
        transaction_type, 
        amount,
        currency,
        channel,  
        transaction_status, 
        timestamp,
        -- Convertimos la zona horaria de sincronización
        CONVERT_TIMEZONE('UTC', _fivetran_synced) AS dateload, 
        -- Normalización de montos
        CAST(amount AS DECIMAL(10, 2)) AS normalized_amount,
        -- Categorización del monto
        CASE
            WHEN amount < 100 THEN 'Low'
            WHEN amount BETWEEN 100 AND 500 THEN 'Medium'
            ELSE 'High'
        END AS amount_category,
        -- Cálculo de días desde la última transacción
        DATEDIFF('day', timestamp, CURRENT_DATE) AS days_since_transaction
    FROM source
    WHERE amount >= 0  -- Filtramos montos negativos
),

-- El resultado final
SELECT * 
FROM renamed

{% if is_incremental() %}
    WHERE timestamp > (SELECT MAX(timestamp) FROM {{ this }})  -- Condición para la carga incremental
{% endif %}
