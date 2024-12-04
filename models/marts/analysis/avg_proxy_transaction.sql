with proxy_transactions as (
    select
        t.transaction_id,
        t.customer_id,
        t.amount_usd,
        t.amount_eur,
        g.is_proxy,
        g.country,
        g.city
    from {{ ref('fct_transaction') }} t
    left join {{ ref('dim_geolocation') }} g
    on t.geo_id = g.geo_id
)
select
    count(transaction_id) as total_transactions,
    sum(amount_usd) as total_amount_usd,
    ROUND(avg(amount_usd),2) as avg_transaction_amount_usd,
    ROUND(avg(amount_eur),2) as avg_amount_eur
from proxy_transactions
where is_proxy = true