{{ config(
    materialized='incremental',
    unique_key='device_id'
) }}

with 
source as (
    select * 
    from {{ ref('stg_bank__device') }} 
),

cleaned as (
    select
        device_id,
        device_type,  
        ip_address,
        customer_id,
        geo_id,
        valid_ip,
        dateload
    from source
)

select * from cleaned

{% if is_incremental() %}
    WHERE dateload > (SELECT MAX(dateload) FROM {{ this }})
{% endif %}
