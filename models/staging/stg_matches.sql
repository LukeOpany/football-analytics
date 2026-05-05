{{ config(materialized='incremental', unique_key='match_id') }}

with source as (
    select * from {{ source('raw', 'match') }}

    {% if is_incremental() %}
        where date::date > (select max(match_date) from {{ this }})
    {% endif %}
),

renamed as (
    select
        id              as match_id,
        match_api_id,
        country_id,
        league_id,
        season,
        stage,
        date::date      as match_date,
        home_team_api_id,
        away_team_api_id,
        home_team_goal,
        away_team_goal
    from source
)

select * from renamed