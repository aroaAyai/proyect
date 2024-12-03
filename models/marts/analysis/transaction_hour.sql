WITH transaction_by_day AS (
    SELECT 
        t.transaction_id,
        t.transaction_date,
        t.amount_usd,
        t.amount_eur,
        t.customer_id,
        t.merchant_id,
        dt.day_of_week_name,  -- Día de la semana en nombre (Sun, Mon, etc.)
        dt.day_of_week_number,  -- Día de la semana en número (1 = Domingo, 2 = Lunes, etc.)
        EXTRACT(YEAR FROM t.transaction_date) AS transaction_year  -- Extraemos el año de la transacción
    FROM {{ ref('fct_transaction') }} t
    JOIN {{ ref('dim_time') }} dt ON t.transaction_date = dt.date  -- Relacionamos la tabla de transacciones con la dimensión de fecha
)

SELECT 
    transaction_year,  -- Añado el año como columna en el resultado
    day_of_week_name,  -- Nombre del día de la semana (Sun, Mon, etc.)
    COUNT(*) AS total_transactions_per_day,  -- Contar las transacciones por día de la semana
    SUM(amount_usd) AS total_amount_per_day,  -- Sumar el monto total por día de la semana
    AVG(amount_usd) AS avg_amount_per_day  -- Promediar el monto por día de la semana
FROM transaction_by_day
GROUP BY transaction_year, day_of_week_name, day_of_week_number, transaction_year -- Agrupar por año, día de la semana y número del día
ORDER BY transaction_year, day_of_week_number, transaction_year