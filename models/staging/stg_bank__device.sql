{{ config(
    materialized='incremental',
    unique_key='device_id'
) }}

with 
source as (
    select * 
    from {{ source('bank', 'device') }}
),

renamed as (
    select
        device_id,
        device_type,
        ip_address,
        customer_id,
        geo_id,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) as dateload 
    from source
    order by device_id asc
)

select * 
from renamed

{% if is_incremental() %}
    WHERE datetime_load_utc > (SELECT MAX(datetime_load_utc) FROM {{ this }})
{% endif %}
