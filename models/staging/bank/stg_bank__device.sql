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
        -- Asegurar que la fecha esté en el formato adecuado y en UTC
        CONVERT_TIMEZONE('UTC', _fivetran_synced) as dateload
    from source
)

select * from cleaned

{% if is_incremental() %}
    WHERE dateload > (SELECT MAX(dateload) FROM {{ this }})
{% endif %}
