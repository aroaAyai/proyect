WITH previous_geo AS (
    SELECT 
        d.device_id,
        d.device_type,
        d.customer_id,
        d.geo_id,
        d.valid_ip,
        d.geo_status,
        d.dateload
    FROM {{ ref('stg_bank__device') }} d 
)

SELECT
    device_id,
    device_type,
    customer_id,
    geo_id,
    valid_ip,
    geo_status,
    dateload,
    COUNT(device_id) OVER (PARTITION BY geo_id) AS device_count,
    COUNT(CASE WHEN geo_status = 'Muy Sospechosa' THEN 1 END) OVER (PARTITION BY geo_id) AS very_suspect_count,
    COUNT(CASE WHEN geo_status = 'Sospechosa' THEN 1 END) OVER (PARTITION BY geo_id) AS suspect_count,
    COUNT(CASE WHEN geo_status = 'Válida' THEN 1 END) OVER (PARTITION BY geo_id) AS valid_count,FROM previous_geo
ORDER BY geo_id, device_id