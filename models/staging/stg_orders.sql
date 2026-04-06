with src as (
    select * from {{ source('raw', 'orders') }}
),
renamed as (
    select
        cast(id as {{ dbt.type_int() }})            as order_id,
        cast(customer_id as {{ dbt.type_int() }})   as customer_id,
        cast(product_id as {{ dbt.type_int() }})    as product_id,
        cast(quantity as {{ dbt.type_int() }})      as quantity,
        cast(unit_price as {{ dbt.type_numeric() }}) as unit_price,
        cast(order_ts as timestamp)                 as order_ts,
        cast(status as {{ dbt.type_string() }})     as order_status
    from src
)
select * from renamed