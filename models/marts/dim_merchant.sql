{{ config(
    materialized='incremental',
    unique_key='merchant_id'
) }}

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
        merchant_risk_score,         
        geo_id,
        dateload,
        average_sale
    FROM source
)

select * 
from renamed

{% if is_incremental() %}
    WHERE dateload > (SELECT MAX(dateload) FROM {{ this }})
{% endif %}
