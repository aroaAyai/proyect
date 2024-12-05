{{ config(materialized="table") }}
with 
source as (
    select * 
    from {{ ref('stg_bank__merchant') }} 
),

renamed as (
    SELECT
        merchant_id,
        merchant_name,
        merchant_category,
        CAST(ROUND(merchant_risk_score) AS INT) AS merchant_risk_score, -- Corregido el casting
        geo_id,
        dateload,
        average_sale
    FROM source

)

select * 
from renamed
