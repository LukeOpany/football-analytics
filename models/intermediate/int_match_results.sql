with matches as (
    select * from {{ ref('stg_matches') }}
),

teams as (
    select * from {{ ref('stg_teams') }}
),

leagues as (
    select * from {{ ref('stg_leagues') }}
),

joined as (
    select
        m.match_id,
        m.match_api_id,
        m.season,
        m.stage,
        m.match_date,
        l.league_name,
        ht.team_long_name   as home_team_name,
        at.team_long_name   as away_team_name,
        m.home_team_api_id,
        m.away_team_api_id,
        m.home_team_goal,
        m.away_team_goal,
        -- Derive result from home team perspective
        case
            when m.home_team_goal > m.away_team_goal  then 'win'
            when m.home_team_goal < m.away_team_goal  then 'loss'
            else 'draw'
        end as home_result,
        m.home_team_goal - m.away_team_goal as goal_difference
    from matches m
    left join teams ht on m.home_team_api_id = ht.team_api_id
    left join teams at on m.away_team_api_id = at.team_api_id
    left join leagues l  on m.league_id = l.league_id
)

select * from joined