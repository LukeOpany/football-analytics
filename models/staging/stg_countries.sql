with source as (
    select * from {{ source('raw', 'country') }}
),

renamed as (
    select
        id          as country_id,
        name        as country_name
    from source
)

select * from renamed