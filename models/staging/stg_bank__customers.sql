{{ config(
    materialized='incremental',
    unique_key='customers_id'
) }}

with 
source as (
    select * 
    from {{ source('bank', 'customers') }}
),

renamed as (
    select
        customer_id,
        name,
        country,
        gender,
        phone_number,
        is_vip,
        CONVERT_TIMEZONE('UTC', registration_date) as registration_date,
        total_spent,
        CONVERT_TIMEZONE('UTC', date_of_birth) as date_birth,
        email,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) as dateload 
    from source
    order by customer_id asc
)

select * 
from renamed

{% if is_incremental() %}
    WHERE datetime_load_utc > (SELECT MAX(datetime_load_utc) FROM {{ this }})
{% endif %}
