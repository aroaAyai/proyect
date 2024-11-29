{{ config(
    materialized='incremental',
    unique_key='account_id'
) }}

with 
source as (
    select * 
    from {{ ref('base_account') }} 
),

cleaned as (
    select 
        account_id,
        lower(account_status) as account_status, 
        lower(account_type) as account_type,  
        date_opened,
        cast(round(overdraft_limit) as int) as overdraft_limit, 
        cast(round(balance) as int) as balance, 
        last_activity,
        datediff('day', last_activity, '2025-01-01') as days_since_last_activity,
        case 
            when round(balance) > 3000 then 'high'
            when round(balance) > 500 then 'medium'
            else 'low'
        end as balance_category,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) as dateload
    from source
    where round(balance) >= 0
        and date_opened <= '2025-01-01'

)

select * 
from cleaned

{% if is_incremental() %}
    where dateload > (select max(dateload) from {{ this }})
{% endif %}
