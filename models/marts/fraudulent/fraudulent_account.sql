{{ config(
    materialized='incremental',
    unique_key='account_id'
) }}

WITH source AS (
    SELECT * 
    FROM {{ ref('dim_account') }} 
),

cleaned_data AS (
    SELECT 
        account_id,
        account_status, 
        days_since_last_activity,
        account_type,  
        date_opened,
        overdraft_limit, 
        balance, 
        last_activity,
        balance_category,
        dateload
    FROM source
),

risk_indicators AS (
    SELECT 
        account_id,
        days_since_last_activity,
        overdraft_limit,
        CASE 
            WHEN balance < 0 THEN 'Sospechosa' -- Cuentas con saldo negativo
            ELSE 'Válida'
        END AS balance_status,
        CASE 
            WHEN days_since_last_activity > 365 THEN 'Muy Sospechosa'
            WHEN days_since_last_activity > 180 THEN 'Sospechosa'
            ELSE 'Válida'
        END AS inactivity_status,
        CASE 
            WHEN overdraft_limit > 0 AND balance < -overdraft_limit THEN 'Sospechosa'
            ELSE 'Válida'
        END AS overdraft_status
    FROM cleaned_data
),

account_risk_classification AS (
    SELECT 
        account_id,
        balance_status,
        inactivity_status,
        days_since_last_activity,
        overdraft_limit,
        overdraft_status,

        (CASE WHEN balance_status = 'Sospechosa' THEN 1 ELSE 0 END +
         CASE WHEN inactivity_status = 'Sospechosa' THEN 1 ELSE 0 END +
         CASE WHEN inactivity_status = 'Muy Sospechosa' THEN 2 ELSE 0 END + -- Mayor peso para "Muy Sospechosa"
         CASE WHEN overdraft_status = 'Sospechosa' THEN 1 ELSE 0 END) AS total_risk,

        CASE 
            WHEN total_risk = 0 THEN 'Sin sospecha'
            WHEN total_risk = 1 THEN 'Sospecha leve'
            WHEN total_risk BETWEEN 2 AND 3 THEN 'Sospecha moderada'
            WHEN total_risk >= 4 THEN 'Fraude'
            ELSE 'Normal'
        END AS risk_classification
    FROM risk_indicators
)

SELECT 
    account_id,
    balance_status,
    inactivity_status,
    days_since_last_activity,
    overdraft_limit,
    overdraft_status,
    total_risk,
    risk_classification
FROM account_risk_classification
order by risk_classification desc