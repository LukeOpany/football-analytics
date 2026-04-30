with all_matches as (
    select
        team_api_id,
        team_name,
        result,
        goals_scored,
        goals_conceded,
        match_id,
        -- figure out if this was a home or away game
        case
            when team_api_id = home_team_api_id then 'home'
            else 'away'
        end as venue
    from {{ ref('int_team_form') }}
    join {{ ref('fct_matches') }} using (match_id)
),

summary as (
    select
        team_name,
        venue,
        count(*)                                            as matches,
        sum(case when result = 'win' then 1 else 0 end)    as wins,
        sum(case when result = 'draw' then 1 else 0 end)   as draws,
        sum(case when result = 'loss' then 1 else 0 end)   as losses,
        round(avg(goals_scored)::numeric, 2)               as avg_goals_scored,
        round(
            sum(case when result = 'win' then 1 else 0 end)::numeric
            / nullif(count(*), 0) * 100
        , 1)                                               as win_pct
    from all_matches
    group by team_name, venue
)

select *
from summary
order by team_name, venue