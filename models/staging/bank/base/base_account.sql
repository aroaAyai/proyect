{{ config(materialized="view") }}
with 
source as (
    select * 
    from {{ ref('account_timestamp_snp_bronze') }}
),

cleaned as (
    select 
        account_id,
        account_status,
        account_type,
        date_opened,
        cast(round(overdraft_limit) as int) as overdraft_limit, 
        cast(round(balance) as int) as balance, 
        last_activity,
        customer_id,
        greatest(datediff('day', last_activity, '2024-12-31'), 0) as days_since_last_activity, 
        case 
            when round(balance) > 3000 then 'high'
            when round(balance) > 500 then 'medium'
            else 'low'
        end as balance_category,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) as dateload
    from source
)

select * 
from cleaned
