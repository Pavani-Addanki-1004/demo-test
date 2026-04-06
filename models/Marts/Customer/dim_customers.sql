with src as (
    select * from {{ ref('stg_customers') }}
)
select
    customer_id,
    email,
    first_name,
    last_name,
    created_at
from src