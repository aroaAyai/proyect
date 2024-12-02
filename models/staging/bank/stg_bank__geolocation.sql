{{ config(
    materialized='incremental',
    unique_key='geo_id'
) }}

with 
source as (
    select * 
    from {{ ref('base_geolocation') }} 
),

renamed as (
    select
        geo_id,
        country,
        city,
        is_proxy,
        dateload
    from source
    order by geo_id asc
)

select * 
from renamed

{% if is_incremental() %}
    WHERE dateload > (SELECT MAX(dateload) FROM {{ this }})
{% endif %}
