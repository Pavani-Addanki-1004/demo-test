with src as (
    select * from {{ ref('stg_products') }}
)
select
    product_id,
    product_name,
    category,
    unit_price,
    created_at
from src