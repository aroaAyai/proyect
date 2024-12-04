WITH transaction_by_day AS (
    SELECT 
        t.transaction_id,
        t.transaction_date,
        t.amount_usd,
        t.amount_eur,
        t.customer_id,
        t.merchant_id,
        dt.day_of_week_name,  
        dt.day_of_week_number, 
        EXTRACT(YEAR FROM t.transaction_date) AS transaction_year 
    FROM {{ ref('fct_transaction') }} t
    JOIN {{ ref('dim_time') }} dt ON t.transaction_date = dt.date
)

SELECT 
    transaction_year, 
    day_of_week_name,  
    COUNT(*) AS total_transactions_per_day, 
    ROUND(SUM(amount_usd),2) AS total_amount_per_day, 
    ROUND(AVG(amount_usd),2) AS avg_amount_per_day 
FROM transaction_by_day
GROUP BY transaction_year, day_of_week_name, day_of_week_number, transaction_year 
ORDER BY transaction_year, day_of_week_number, transaction_year