{{ config(
    materialized='view'  
) }}

with 
source as (
    select * 
    from {{ source('bank', 'account') }}
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
        datediff('day', last_activity, '2024-01-31') as days_since_last_activity,
        case 
            when round(balance) > 3000 then 'high'
            when round(balance) > 500 then 'medium'
            else 'low'
        end as balance_category,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) as dateload
    from source
    where round(balance) >= 0
        and date_opened <= '2024-01-31'

)

select * 
from cleaned
