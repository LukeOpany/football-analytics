with source as (
    select * from {{ source('raw', 'player') }}
),

renamed as (
    select
        id                  as player_id,
        player_api_id,
        player_fifa_api_id    ::int as player_fifa_api_id,
        player_name,
        birthday            ::date as birthday,
        height,
        weight
    from source
)

select * from renamed