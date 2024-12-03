{{ 
    config(
        materialized='table', 
        sort='date_day',
        dist='date_day',
        pre_hook="alter session set timezone = 'Europe/Madrid'; alter session set week_start = 7;" 
    ) 
}}

WITH date AS (
    {{ dbt_utils.date_spine(
        datepart="day", 
        start_date="cast('1900-01-01' as date)", 
        end_date="cast(current_date() + 1 as date)"
    ) }}
)

SELECT
    date_day AS date,
    year(date_day) * 10000 + month(date_day) * 100 + day(date_day) AS id_date,
    year(date_day) AS year,
    month(date_day) AS month,
    monthname(date_day) AS desc_month,
    year(date_day) * 100 + month(date_day) AS id_anio_mes,
    quarter(date_day) AS quarter,
    date_day - 1 AS dia_previo,  -- El día previo
    concat(year(date_day), weekiso(date_day), dayofweek(date_day)) AS anio_semana_dia,
    weekiso(date_day) AS semana_anio,
    dayofweek(date_day) AS day_of_week_number,
    dayname(date_day) AS day_of_week_name,
    dayofyear(date_day) AS day_of_year,
    CASE 
        WHEN dayofweek(date_day) IN (6, 7) THEN 'Sí'  -- Fin de semana
        ELSE 'No' 
    END AS is_weekend,
    current_timestamp AS current_time,  -- Fecha y hora actuales
    EXTRACT(HOUR FROM current_timestamp) AS hora  -- Extrae la hora actual
FROM date
ORDER BY date_day DESC
