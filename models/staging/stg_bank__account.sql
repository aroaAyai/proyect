{{ config(
    materialized='incremental',
    unique_key='account_id'
) }}

with 
source as (
    select * 
    from {{ source('bank', 'account') }}
),

renamed as (
    select
        account_id,
        account_type,
        date_opened,
        account_status,
        customer_id,
        overdraft_limit,
        balance,
        last_activity,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) as dateload 
    from source
    order by account_id asc
)

select * 
from renamed

{% if is_incremental() %}
    WHERE datetime_load_utc > (SELECT MAX(datetime_load_utc) FROM {{ this }})
{% endif %}
