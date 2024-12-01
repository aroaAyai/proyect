{{ config(
    materialized='incremental',
    unique_key='merchant_id'
) }}

with 
source as (
    select * 
    from {{ source('bank','merchant') }} 
),

renamed as (
    SELECT
        merchant_id,
        merchant_name,
        merchant_category,
        CAST(TRUNCATE(merchant_risk_score, 0) AS INT) AS merchant_risk_score, 
        geo_id,
        average_sale,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) AS dateload
    FROM source
    order by merchant_id asc
)

select * 
from renamed

{% if is_incremental() %}
    WHERE dateload > (SELECT MAX(dateload) FROM {{ this }})
{% endif %}
