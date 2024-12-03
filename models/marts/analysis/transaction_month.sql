WITH transaction_by_month AS (
    SELECT 
        t.transaction_id,
        t.transaction_date,
        t.amount_usd,
        t.amount_eur,
        t.customer_id,
        t.merchant_id,
        EXTRACT(MONTH FROM dt.date) AS transaction_month,  -- Extraer el mes de la fecha
        EXTRACT(YEAR FROM dt.date) AS transaction_year    -- Extraer el año de la fecha
    FROM {{ ref('fct_transaction') }} t
    JOIN {{ ref('dim_time') }} dt ON t.transaction_date = dt.date
)

SELECT 
    transaction_year,
    transaction_month,
    COUNT(transaction_id) AS total_transactions_month,
    ROUND(SUM(amount_usd), 2) AS total_amount_month_usd,
    ROUND(AVG(amount_usd), 2) AS avg_amount_month_usd,
    ROUND(SUM(amount_eur), 2) AS total_amount_month_eur,
    ROUND(AVG(amount_eur), 2) AS avg_amount_month_eur    
FROM transaction_by_month
GROUP BY 
    transaction_year,
    transaction_month
ORDER BY 
    transaction_year DESC, 
    transaction_month DESC
