with source as (
    select * from {{ source('raw', 'league') }}
),

renamed as (
    select
        id          as league_id,
        country_id,
        name        as league_name
    from source
)

select * from renamed