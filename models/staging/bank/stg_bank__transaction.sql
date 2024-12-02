with 
source as (
    select * 
    from {{ source('bank', 'transaction') }}
),

renamed as (
    select
        transaction_id,
        account_id, 
        merchant_id, 
        device_id,
        transaction_type, 
        amount,
        currency,
        CASE 
            WHEN channel IS NULL THEN 'Online' 
            ELSE channel  
        END AS channel, 
        CASE 
            WHEN transaction_status IS NULL THEN 'No aceptada' 
            ELSE transaction_status  
        END AS transaction_status,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) AS dateload, 
    from source
    order by transaction_id asc
)

select 
    transaction_id,
    account_id,
    merchant_id,
    device_id,
    transaction_type,
    amount as amount_usd,
    ROUND(CASE
        WHEN currency = 'USD' THEN amount * 0.95
        ELSE amount
    END, 2) AS amount_eur, 
    channel,
    transaction_status,
    dateload
from renamed

{% if is_incremental() %}
    WHERE dateload > (SELECT MAX(dateload) FROM {{ this }}) 
{% endif %}
