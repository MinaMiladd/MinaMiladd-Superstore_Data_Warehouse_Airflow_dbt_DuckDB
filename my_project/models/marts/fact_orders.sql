
select
    s.row_id           as sales_key,
    s.order_id,

    od.date_key        as order_date_key,
    sd.date_key        as ship_date_key,
    c.customer_key,
    p.product_key,
    g.geography_key,

    s.ship_mode,
    s.sales,
    s.quantity,
    s.discount,
    s.profit
from {{ ref('stg_superstore') }} s

left join {{ ref('dim_customer') }} c
    on s.customer_id = c.customer_id

left join {{ ref('dim_product') }} p
    on  s.product_id   = p.product_id
    and s.product_name = p.product_name

left join {{ ref('dim_location') }} g
    on  s.country = g.country
    and s.region  = g.region
    and s.state   = g.state
    and s.city    = g.city
    and s.postal_code is not distinct from g.postal_code

left join {{ ref('dim_date') }} od
    on s.order_date = od.date_day

left join {{ ref('dim_date') }} sd
    on s.ship_date = sd.date_day