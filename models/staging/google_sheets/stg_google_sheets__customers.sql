WITH source AS (
    SELECT *
    FROM {{ ref('_base_customers') }} 
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
        registration_date,
        total_spent,
        date_of_birth,
        email,
        CASE 
            WHEN country = 'USA' AND LENGTH(REGEXP_REPLACE(phone_number, '^\+1', '')) BETWEEN 9 AND 10 THEN 'VALID'
            WHEN country = 'MEXICO' AND LENGTH(REGEXP_REPLACE(phone_number, '^\+52', '')) = 10 THEN 'VALID'
            WHEN country = 'SPAIN' AND LENGTH(REGEXP_REPLACE(phone_number, '^\+34', '')) = 9 THEN 'VALID'
            WHEN country = 'GERMANY' AND LENGTH(REGEXP_REPLACE(phone_number, '^\+49', '')) BETWEEN 9 AND 11 THEN 'VALID'
            WHEN country = 'AUSTRALIA' AND LENGTH(REGEXP_REPLACE(phone_number, '^\+61', '')) BETWEEN 9 AND 11 THEN 'VALID'
            WHEN country = 'CANADA' AND LENGTH(REGEXP_REPLACE(phone_number, '^\+1', '')) BETWEEN 9 AND 11 THEN 'VALID'
            WHEN country = 'UK' AND LENGTH(REGEXP_REPLACE(phone_number, '^\+44', '')) BETWEEN 9 AND 11 THEN 'VALID'
            WHEN country = 'FRANCE' AND LENGTH(REGEXP_REPLACE(phone_number, '^\+33', '')) = 9 THEN 'VALID'
            ELSE 'INVALID'
        END AS phone_validation_status,
        age_years
    FROM cleaned
)

SELECT *
FROM cleaned
