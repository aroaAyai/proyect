{{ config(
    materialized='view'
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
        case 
            when gender in ('M', 'Male') then 'M'  -- 'M' o 'Male' se normaliza como 'M'
            when gender in ('F', 'Female') then 'F'  -- 'F' o 'Female' se normaliza como 'F'
            when gender in ('NB', 'Non-Binary') then 'NB'  -- 'Non-Binary' se normaliza como 'NB'
            else 'Unknown'  -- Cualquier otro valor se clasifica como 'Unknown'
        end as gender,
        case
            when total_spent <= 3500 then 'low spender'
            when total_spent > 500 and total_spent <= 2000 then 'medium spender'
            else 'high spender'
        end as spending_category, 
        phone_number,
        CONVERT_TIMEZONE('UTC', registration_date) as registration_date,
        total_spent,
        CONVERT_TIMEZONE('UTC', date_of_birth) as date_of_birth,
        email,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) as dateload 
    from source
    order by customer_id asc
)

select * 
from renamed
