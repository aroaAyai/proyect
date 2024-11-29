{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

with 
source as (
    select * 
    from {{ ref('base_customers') }} 
),

cleaned as (
    select
        customer_id,
        lower(name) as name,  
        upper(country) as country, 
        gender,
        phone_number, 
        spending_category, 
        registration_date,
        total_spent,
        date_of_birth,
        email,
        dateload 
    from source
    where email is not null  
),

age_calculation as (
    select
        customer_id,
        name,
        country,
        gender,
        phone_number,
        spending_category,
        registration_date,
        total_spent,
        date_of_birth,
        email,
        dateload,
        -- Cálculo de la edad del cliente redondeado (en años)
        ROUND(DATEDIFF('day', date_of_birth, current_date) / 365.0) as age_years
    from cleaned
)

select * 
from age_calculation
