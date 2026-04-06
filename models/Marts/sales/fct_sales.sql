with src as (
    select * from {{ ref('int_orders_enriched') }}
)
select
    order_id,
    order_date,
    customer_id,
    product_id,
    category,
    quantity,
    unit_price_effective as unit_price,
    revenue,
    order_status
from src