{{ config(materialized="view") }}

WITH source AS (
    SELECT *
    FROM {{ ref('base_customers') }} 
),

geolocation_data AS (
    SELECT *
    FROM {{ ref('base_geolocation') }}
),

cleaned AS (
    SELECT
        c.customer_id,
        g.country AS country,
        c.gender,
        c.name,
        g.geo_id,
        c.phone_number,
        c.registration_date,
        c.total_spent,
        c.date_of_birth,
        c.email,
        c.age_years
    FROM source c
    LEFT JOIN geolocation_data g 
    ON c.geo_id = g.geo_id
),

data AS (
    SELECT
        customer_id,
        name,
        country,
        gender,
        phone_number,
        geo_id,
        registration_date,
        total_spent,
        date_of_birth,
        email,
        CASE 
            WHEN country = 'USA' AND LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(phone_number, '[^0-9]', ''), '^\+1', '')) BETWEEN 9 AND 10 THEN 'VALID'
            WHEN country = 'Mexico' AND LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(phone_number, '[^0-9]', ''), '^\+5', '')) BETWEEN 9 AND 10 THEN 'VALID'
            WHEN country = 'Spain' AND LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(phone_number, '[^0-9]', ''), '^\+34', '')) = 9 THEN 'VALID'
            WHEN country = 'Germany' AND LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(phone_number, '[^0-9]', ''), '^\+49', '')) BETWEEN 9 AND 12 THEN 'VALID'
            WHEN country = 'Ausrtalia' AND LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(phone_number, '[^0-9]', ''), '^\+61', '')) BETWEEN 9 AND 11 THEN 'VALID'
            WHEN country = 'Canada' AND LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(phone_number, '[^0-9]', ''), '^\+1', '')) BETWEEN 9 AND 11 THEN 'VALID'
            WHEN country = 'UK' AND LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(phone_number, '[^0-9]', ''), '^\+44', '')) BETWEEN 9 AND 11 THEN 'VALID'
            WHEN country = 'France' AND LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(phone_number, '[^0-9]', ''), '^\+33', '')) BETWEEN 9 AND 10 THEN 'VALID'
            ELSE 'INVALID'
        END AS phone_validation_status,
        age_years
    FROM cleaned
)

SELECT *
FROM data
