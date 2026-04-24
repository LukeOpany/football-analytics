with teams as (
    select * from {{ ref('stg_teams') }}
),

-- Aggregate match performance per team
match_stats as (
    select
        team_api_id,
        count(*)                                        as total_matches,
        sum(case when result = 'win'  then 1 else 0 end) as total_wins,
        sum(case when result = 'draw' then 1 else 0 end) as total_draws,
        sum(case when result = 'loss' then 1 else 0 end) as total_losses,
        sum(goals_scored)                               as total_goals_scored,
        sum(goals_conceded)                             as total_goals_conceded
    from {{ ref('int_team_form') }}
    group by team_api_id
),

joined as (
    select
        t.team_id,
        t.team_api_id,
        t.team_long_name,
        t.team_short_name,
        ms.total_matches,
        ms.total_wins,
        ms.total_draws,
        ms.total_losses,
        ms.total_goals_scored,
        ms.total_goals_conceded,
        ms.total_goals_scored - ms.total_goals_conceded as overall_goal_difference,
        ms.total_wins * 3 + ms.total_draws              as total_points
    from teams t
    left join match_stats ms on t.team_api_id = ms.team_api_id
)

select * from joined