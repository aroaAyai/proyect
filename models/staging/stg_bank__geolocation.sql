{{ config(
    materialized='incremental',
    unique_key='geo_id'
) }}

with 
source as (
    select * 
    from {{ source('bank', 'geolocation') }}
),

renamed as (
    select
        geo_id,
        country,
        city,
        is_proxy,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) as dateload 
    from source
    order by geo_id asc
)

select * 
from renamed

{% if is_incremental() %}
    WHERE datetime_load_utc > (SELECT MAX(datetime_load_utc) FROM {{ this }})
{% endif %}
