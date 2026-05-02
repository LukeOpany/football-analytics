-- change these two variables to explore different points in time
-- season format: '2015/2016'
-- up_to_date: any date in that season

with filtered_matches as (
    select *
    from {{ ref('fct_matches') }}
    where season = '2015/2016'
      and match_date <= '2016-01-01'   -- standings as of this date
),

home_points as (
    select
        home_team_name  as team_name,
        home_team_goal  as goals_for,
        away_team_goal  as goals_against,
        case home_result
            when 'win'  then 3
            when 'draw' then 1
            else 0
        end             as points
    from filtered_matches
),

away_points as (
    select
        away_team_name  as team_name,
        away_team_goal  as goals_for,
        home_team_goal  as goals_against,
        case home_result
            when 'loss' then 3
            when 'draw' then 1
            else 0
        end             as points
    from filtered_matches
),

all_points as (
    select * from home_points
    union all
    select * from away_points
),

standings as (
    select
        team_name,
        count(*)                    as played,
        sum(points)                 as points,
        sum(goals_for)              as goals_for,
        sum(goals_against)          as goals_against,
        sum(goals_for)
            - sum(goals_against)    as goal_difference,
        sum(case when points = 3 then 1 else 0 end) as wins,
        sum(case when points = 1 then 1 else 0 end) as draws,
        sum(case when points = 0 then 1 else 0 end) as losses
    from all_points
    group by team_name
)

select
    rank() over (order by points desc, goal_difference desc) as position,
    team_name,
    played,
    wins,
    draws,
    losses,
    goals_for,
    goals_against,
    goal_difference,
    points
from standings
order by position