{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

with 
source as (
    select * 
    from {{ source('bank', 'transaction') }}  -- Fuentes de datos de la tabla de transacciones
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
        channel,  
        transaction_status, 
        timestamp,
        _fivetran_synced, 
        CONVERT_TIMEZONE('UTC', _fivetran_synced) as dateload  -- Convertimos la zona horaria de sincronizació
    from source
    order by transaction_id asc
)

select * 
from renamed

{% if is_incremental() %}
    WHERE timestamp > (SELECT MAX(timestamp) FROM {{ this }})  -- Condición de carga incremental basada en la fecha de transacción
{% endif %}
