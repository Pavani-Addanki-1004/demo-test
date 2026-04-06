-- models/stg_orders.sql
SELECT
    order_id,
    customer_id,
    order_date,
    status,
    total_amount,
    is_returned,
    created_at,
    updated_at
FROM {{ source('raw', 'orders') }}
