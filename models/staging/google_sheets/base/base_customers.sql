WITH source AS (
    SELECT *
    FROM {{ ref('customer_timestamp_snp_bronze') }} 
),


age_calculation AS (
    SELECT
        customer_id,
        name,
        geo_id,
        gender,
        phone_number,
        registration_date,
        total_spent,
        date_of_birth,
        email,
        ROUND(DATEDIFF('day', date_of_birth, current_date) / 365.0) AS age_years
    FROM source
)

SELECT *
FROM age_calculation
