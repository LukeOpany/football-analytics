with source as (
    select * from {{ source('raw', 'team') }}
),

renamed as (
    select
        id                  as team_id,
        team_api_id,
        team_fifa_api_id    ::int as team_fifa_api_id,
        team_long_name,
        team_short_name
    from source
)

select * from renamed