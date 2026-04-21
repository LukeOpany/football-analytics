with match_results as (
    select * from {{ ref('int_match_results') }}
),

-- Unpivot: one row per team per match (home and away separately)
home_matches as (
    select
        match_id,
        match_date,
        season,
        home_team_api_id    as team_api_id,
        home_team_name      as team_name,
        home_result         as result,
        home_team_goal      as goals_scored,
        away_team_goal      as goals_conceded
    from match_results
),

away_matches as (
    select
        match_id,
        match_date,
        season,
        away_team_api_id    as team_api_id,
        away_team_name      as team_name,
        case
            when home_result = 'win'  then 'loss'
            when home_result = 'loss' then 'win'
            else 'draw'
        end                 as result,
        away_team_goal      as goals_scored,
        home_team_goal      as goals_conceded
    from match_results
),

all_matches as (
    select * from home_matches
    union all
    select * from away_matches
),

with_points as (
    select
        *,
        case
            when result = 'win'  then 3
            when result = 'draw' then 1
            else 0
        end as points,
        -- Row number per team ordered by date (used for form window)
        row_number() over (
            partition by team_api_id
            order by match_date
        ) as match_number
    from all_matches
),

with_form as (
    select
        *,
        -- Points from last 5 games
        sum(points) over (
            partition by team_api_id
            order by match_date
            rows between 4 preceding and current row
        ) as form_points_last_5,
        -- Goals scored in last 5 games
        sum(goals_scored) over (
            partition by team_api_id
            order by match_date
            rows between 4 preceding and current row
        ) as form_goals_scored_last_5,
        -- Previous match result (LAG)
        lag(result, 1) over (
            partition by team_api_id
            order by match_date
        ) as prev_match_result
    from with_points
)

select * from with_form