{{ config(
    materialized='incremental',
    unique_key='account_id'
) }}

with 
source as (
    select * 
    from {{ ref('stg_bank__account') }} 
),

cleaned as (
    select 
        account_id,
        account_status, 
        account_type,  
        date_opened,
        overdraft_limit, 
        balance, 
        last_activity,
        days_since_last_activity,
        balance_category,
        dateload
    from source

)

select * 
from cleaned

{% if is_incremental() %}
    where dateload > (select max(dateload) from {{ this }})
{% endif %}
