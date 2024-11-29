{{ config(
    materialized='incremental',
    unique_key='device_id'
) }}

with 
source as (
    select * 
    from {{ source('bank', 'device') }} 
),

cleaned as (
    select
        device_id,
        upper(device_type) as device_type,  
        ip_address,
        customer_id,
        geo_id,
        case 
            when ip_address like '%.%.%.%' then ip_address  -- Asegurar que la IP tenga el formato adecuado
            else null  -- Si no, se marca como NULL
        end as valid_ip,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) as dateload,
    from source

),

geo_info as (

    select 
        geo_id  
    from {{ source('bank','geolocation') }}  -- Tabla de referencia para geolocalización
),

joined as (
    select
        c.device_id,
        c.device_type,
        c.valid_ip,
        c.customer_id,
        c.dateload,
        g.geo_id  
    from cleaned c
    left join geo_info g
        on c.geo_id = g.geo_id

)

select 
    device_id,
    device_type,
    valid_ip,
    customer_id,
    geo_id  -- Solo devolvemos geo_id
from joined

{% if is_incremental() %}
    WHERE dateload > (SELECT MAX(dateload) FROM {{ this }})
{% endif %}
