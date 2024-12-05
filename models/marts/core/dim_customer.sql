{{ config(materialized="table") }}

WITH source AS (
    SELECT *
    FROM {{ ref('stg_google_sheets__customers') }} 
),

data AS (
    SELECT
        customer_id,           
        name,
        geo_id,             
        country,        
        gender,       
        phone_number,       
        registration_date,   
        total_spent,     
        date_of_birth,      
        email,
        phone_validation_status,
        age_years
    FROM source

)

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
    phone_validation_status, 
    age_years           
FROM data
