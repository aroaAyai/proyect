{{ config(
    materialized='table',
) }}

WITH source AS (
    SELECT 
        t.customer_id,
        t.name,
        t.risk_classification 
    FROM {{ ref('fraudulent_customer') }} t
)

SELECT 
    customer_id, 
    name,
    'Posible fraude' AS fraudulencia  
FROM source
WHERE risk_classification = 'Sospecha moderada'
GROUP BY customer_id, name 
ORDER BY customer_id DESC