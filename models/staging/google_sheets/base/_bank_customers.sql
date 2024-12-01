with source as (
    select * 
    from {{ source('google_sheets', 'customers') }} 
),

-- Join customers with geolocation data to get country based on geo_id
geolocation_data as (
    select 
        geo_id, 
        country
    from {{ source('bank', 'geolocation') }} 
),

cleaned as (
    select
        c.customer_id,
        lower(c.name) as name,  
        upper(g.country) as country,  -- Use country from geolocation data
        c.gender,
        phone_number, 
        c.registration_date,
        c.total_spent,
        c.date_of_birth,
        c.email
    from source c
    -- Join with geolocation data on geo_id
    inner join geolocation_data g on c.geo_id = g.geo_id
),

validated as (
    select
        customer_id,
        name,
        country,
        gender,
        phone_number,
        registration_date,
        total_spent,
        date_of_birth,
        email,
        -- Verificación de la longitud del teléfono sin el prefijo
        case 
            when country = 'USA' and length(regexp_replace(phone_number, '^\+1', '')) = 10 then 'VALID'
            when country = 'MEXICO' and length(regexp_replace(phone_number, '^\+52', '')) = 10 then 'VALID'
            when country = 'SPAIN' and length(regexp_replace(phone_number, '^\+34', '')) = 9 then 'VALID'
            when country = 'GERMANY' and length(regexp_replace(phone_number, '^\+49', '')) between 10 and 11 then 'VALID'
            when country = 'AUSTRALIA' and length(regexp_replace(phone_number, '^\+61', '')) = 9 then 'VALID'
            when country = 'CANADA' and length(regexp_replace(phone_number, '^\+1', '')) = 10 then 'VALID'
            when country = 'UK' and length(regexp_replace(phone_number, '^\+44', '')) = 10 then 'VALID'
            when country = 'FRANCE' and length(regexp_replace(phone_number, '^\+33', '')) = 9 then 'VALID'
            else 'INVALID'
        end as phone_validation_status
    from cleaned
),

age_calculation as (
    select
        customer_id,
        name,
        country,
        gender,
        -- Usar el número de teléfono actualizado
        phone_number,
        phone_validation_status,
        registration_date,
        total_spent,
        date_of_birth,
        email,
        -- Cálculo de la edad del cliente redondeado (en años)
        ROUND(DATEDIFF('day', date_of_birth, current_date) / 365.0) as age_years
    from validated
)

select * 
from age_calculation
