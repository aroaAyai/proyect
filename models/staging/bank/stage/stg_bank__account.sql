{{ config(
    materialized='incremental',
    unique_key='account_id'
) }}

WITH source AS (
    SELECT * 
    FROM {{ ref('base_account') }} 
),

cleaned AS (
    SELECT 
        account_id,
        CASE 
            WHEN account_status IS NULL THEN 'inactiva'
            ELSE lower(account_status)  
        END AS account_status,
        
        CASE 
            WHEN account_type IS NULL THEN 'corriente'
            WHEN lower(account_type) = 'corriente' THEN 'corriente' 
            ELSE lower(account_type)  
        END AS account_type,
        
        date_opened,
        
        CAST(ROUND(overdraft_limit) AS INT) AS overdraft_limit, 
        
        CASE 
            WHEN balance IS NULL THEN 0 
            WHEN balance = 0 THEN 0 
            ELSE CAST(ROUND(balance) AS INT)  
        END AS balance,
        
        last_activity,
        
        DATEDIFF('day', last_activity, '2025-01-01') AS days_since_last_activity,
        
        CASE 
            WHEN ROUND(balance) > 3000 THEN 'high'
            WHEN ROUND(balance) > 500 THEN 'medium'
            ELSE 'low'
        END AS balance_category,
        
        dateload
    FROM source
    WHERE ROUND(balance) >= 0
        AND date_opened <= '2025-01-01'
)

SELECT * 
FROM cleaned

{% if is_incremental() %}
    WHERE dateload > (SELECT MAX(dateload) FROM {{ this }})
{% endif %}
