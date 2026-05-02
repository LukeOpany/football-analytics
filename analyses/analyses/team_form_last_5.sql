with latest_form as (
    select distinct on (team_api_id)
        team_name,
        match_date,
        form_points_last_5,
        form_goals_scored_last_5,
        result          as last_result,
        prev_match_result
    from {{ ref('int_team_form') }}
    order by team_api_id, match_date desc
)

select
    team_name,
    match_date          as last_match_date,
    form_points_last_5  as points_last_5_games,
    form_goals_scored_last_5 as goals_last_5_games,
    last_result,
    prev_match_result
from latest_form
order by form_points_last_5 desc