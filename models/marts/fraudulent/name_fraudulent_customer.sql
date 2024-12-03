{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

WITH source AS (
    SELECT 
        t.customer_id,
        t.name,
        t.risk_classification  -- Asegúrate de que este campo esté disponible en la fuente de datos
    FROM {{ ref('fraudulent_customer') }} t
    {% if is_incremental() %}
      AND t.time_key > (SELECT MAX(time_key) FROM {{ this }})
    {% endif %}
)

SELECT 
    customer_id, 
    name,
    'Fraude' AS fraudulencia  
FROM source
WHERE risk_classification = 'Sospecha moderada'
GROUP BY customer_id, name 
ORDER BY customer_id DESC