with source as (
    select * from {{ source('raw', 'match') }}
),

renamed as (
    select
        id                  as match_id,
        match_api_id,
        country_id,
        league_id,
        season,
        stage,
        date                ::date as match_date,
        home_team_api_id,
        away_team_api_id,
        home_team_goal,
        away_team_goal
    from source
)

select * from renamed