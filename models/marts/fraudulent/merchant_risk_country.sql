with merchant_risk as (
    select
        m.merchant_id,
        m.merchant_name,
        m.merchant_category,
        m.merchant_risk_score,
        g.country,
        m.average_sale
    from {{ ref('dim_merchant') }} m
    left join {{ ref('dim_geolocation') }} g
    on m.geo_id = g.geo_id
),
country_risk_count as (
    select 
        country,
        COUNT(merchant_id) as high_risk_merchants_count,
        RANK() OVER (ORDER BY COUNT(merchant_id) DESC) as rank
    from merchant_risk
    where merchant_risk_score > 80
    group by country
)
select country, high_risk_merchants_count
from country_risk_count
where rank = 1