{{ config(
    materialized='view'
) }}

with 
source as (
    select * 
    from {{ source('bank', 'transaction') }} 
)

select 
    transaction_id,
    account_id, 
    merchant_id, 
    device_id,
    transaction_type, 
    amount,
    currency,
    channel,  
    transaction_status, 
    timestamp,
    _fivetran_synced
from source
