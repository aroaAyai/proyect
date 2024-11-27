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
    select
        merchant_id,
        merchant_name,
        merchant_category,
        merchant_risk_score,
        geo_id,
        average_sale,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) as dateload 
    from source
    order by merchant_id asc
)

select * 
from renamed

{% if is_incremental() %}
    WHERE datetime_load_utc > (SELECT MAX(datetime_load_utc) FROM {{ this }})
{% endif %}
