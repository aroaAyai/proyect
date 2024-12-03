WITH source AS (
    SELECT *
    FROM {{ source('google_sheets', 'customers') }} 
),

age_calculation AS (
    SELECT
        customer_id,
        name,
        geo_id
    FROM source
),

device_source AS (
    SELECT * 
    FROM {{ source('bank', 'device') }} 
),

cleaned AS (
    SELECT
        device_id,
        -- Asigna "SIN DISPOSITIVO" si el tipo de dispositivo es NULL
        CASE 
            WHEN device_type IS NULL THEN 'SIN DISPOSITIVO'
            ELSE UPPER(device_type)  
        END AS device_type,
        ip_address,
        customer_id,
        geo_id,
        CASE 
            WHEN ip_address LIKE '%.%.%.%' THEN ip_address  
            ELSE NULL  
        END AS valid_ip,
        -- Convertir el tiempo de sincronización a la zona UTC
        CONVERT_TIMEZONE('UTC', _fivetran_synced) AS dateload
    FROM device_source
),

geo_status_calculation AS (
    SELECT
        ac.*,
        d.device_id,
        d.device_type,
        d.valid_ip,
        d.dateload,
        -- Clasificar estado de IP y geo
        CASE
            WHEN d.valid_ip IS NULL AND ac.geo_id != d.geo_id THEN 'Muy Sospechosa'
            WHEN d.valid_ip IS NULL THEN 'Sospechosa'
            WHEN ac.geo_id != d.geo_id THEN 'Sospechosa'
            ELSE 'Válida'
        END AS geo_status
    FROM age_calculation ac
    INNER JOIN cleaned d
        ON ac.customer_id = d.customer_id
)

SELECT *
FROM geo_status_calculation
-- Lógica incremental
{% if is_incremental() %}
WHERE dateload > (
    SELECT MAX(dateload)
    FROM {{ this }}
)
{% endif %}
