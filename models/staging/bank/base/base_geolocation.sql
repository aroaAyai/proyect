{{ config(materialized="view") }}

with 
source as (
    select * 
    from {{ source('bank','geolocation') }} 
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
