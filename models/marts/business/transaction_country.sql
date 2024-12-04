WITH international_transactions AS (
    SELECT 
        t.customer_id,
        c.name,
        g.country AS country,
        COUNT(t.transaction_id) AS total_transactions,
        SUM(t.amount_usd) AS total_spent_usd,
        AVG(t.amount_usd) AS avg_transaction_value
    FROM {{ ref('fct_transaction') }} t
    JOIN {{ ref('dim_customer') }} c ON t.customer_id = c.customer_id
    JOIN {{ ref('dim_geolocation') }} g ON t.geo_id = g.geo_id
    GROUP BY t.customer_id, c.name, g.country
)

SELECT 
    country,
    SUM(total_transactions) AS total_transactions, 
    ROUND(SUM(total_spent_usd), 2) AS total_spent_usd, 
    ROUND(AVG(avg_transaction_value), 2) AS avg_transaction_value
FROM international_transactions
GROUP BY country
ORDER BY total_transactions DESC