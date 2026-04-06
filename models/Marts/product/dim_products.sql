with src as (
    select * from {{ ref('stg_productdata') }}
)
select
    product_id,
    product_name,
    category,
    unit_price,
    created_at
from src