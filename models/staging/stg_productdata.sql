with src as (
    select * from {{ source('raw', 'products') }}
),
renamed as (
    select
        cast(id as {{ dbt.type_int() }})            as product_id,
        trim(product_name)                          as product_name,
        trim(category)                              as category,
        cast(price as {{ dbt.type_numeric() }})     as unit_price,
        cast(created_at as timestamp)               as created_at
    from src
)
select * from renamed