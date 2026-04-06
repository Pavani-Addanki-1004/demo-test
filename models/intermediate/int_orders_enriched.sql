with
orders as (select * from {{ ref('stg_orders') }}),
customers as (select * from {{ ref('stg_customerdata') }}),
products as (select * from {{ ref('stg_productdata') }}),

joined as (
    select
        o.order_id,
        o.order_ts,
        date_trunc('day', o.order_ts) as order_date,
        o.customer_id,
        c.email,
        c.first_name,
        c.last_name,
        o.product_id,
        p.product_name,
        p.category,
        o.quantity,
        coalesce(o.unit_price, p.unit_price) as unit_price_effective,
        o.quantity * coalesce(o.unit_price, p.unit_price) as revenue,
        o.order_status
    from orders o
    left join customers c on o.customer_id = c.customer_id
    left join products p on o.product_id  = p.product_id
)
select * from joined