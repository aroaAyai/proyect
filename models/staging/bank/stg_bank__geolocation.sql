{{ config(materialized="view") }}

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
