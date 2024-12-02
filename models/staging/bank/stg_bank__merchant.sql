{{ config(
    materialized='incremental',
    unique_key='merchant_id'
) }}

with 
source as (
    select * 
    from {{ source('bank', 'merchant') }} 
),

renamed as (
    SELECT
        merchant_id,
        merchant_name,
        -- Manejo de 'NULL' para merchant_category
        CASE 
            WHEN merchant_category IS NULL THEN 'sin categoria'
            ELSE merchant_category
        END AS merchant_category,
        
        -- Mantener merchant_risk_score sin cambios (puedes redondear si es necesario)
        merchant_risk_score, 
        
        geo_id,
        
        -- Conversión de zona horaria
        CONVERT_TIMEZONE('UTC', _fivetran_synced) AS dateload,
        
        -- Asegúrate de que 'AVERAGE_SALE' esté en la transformación
        average_sale
    FROM source
    ORDER BY merchant_id ASC
)

select * 
from renamed

{% if is_incremental() %}
    -- Filtrar solo los registros nuevos o actualizados
    WHERE dateload > (SELECT MAX(dateload) FROM {{ this }})
{% endif %}
