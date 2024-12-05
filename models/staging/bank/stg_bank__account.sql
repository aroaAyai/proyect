{{ config(materialized="view") }}

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
        days_since_last_activity,
        balance_category,
        customer_id,
        dateload
    from source
    where round(balance) >= 0
        and date_opened <= '2025-01-01'

)

select * 
from cleaned


