with src as (
    select * from {{ source('raw', 'customers') }}
),
renamed as (
    select
        cast(id as {{ dbt.type_int() }})            as customer_id,
        lower(trim(email))                          as email,
        initcap(trim(first_name))                   as first_name,
        initcap(trim(last_name))                    as last_name,
        cast(created_at as timestamp)               as created_at,
        cast(updated_at as timestamp)               as updated_at
    from src
)
select * from renamed