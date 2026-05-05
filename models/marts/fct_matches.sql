with match_results as (
    select * from {{ ref('int_match_results') }}
)

select
    match_id,
    match_api_id,
    season,
    stage,
    match_date,
    league_name,
    home_team_name,
    away_team_name,
    home_team_api_id,
    away_team_api_id,
    home_team_goal,
    away_team_goal,
    home_result,
    goal_difference,
    -- Away result is just the inverse
    {{ flip_result('home_result') }} as away_result
from match_results
