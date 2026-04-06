-- models/stg_customers.sql
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    country,
    plan_type,
    is_active,
    signup_date,
    last_login_at
FROM {{ source('raw', 'customers') }}
