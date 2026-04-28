with leagues as (
    select * from {{ ref('stg_leagues') }}
),

countries as (
    select * from {{ ref('stg_countries') }}
),

match_stats as (
    select
        league_name,
        count(*)                        as total_matches,
        count(distinct season)          as total_seasons,
        avg(home_team_goal + away_team_goal) as avg_goals_per_match,
        sum(home_team_goal + away_team_goal) as total_goals
    from {{ ref('fct_matches') }}
    group by league_name
),

joined as (
    select
        l.league_id,
        l.league_name,
        c.country_name,
        ms.total_matches,
        ms.total_seasons,
        round(ms.avg_goals_per_match::numeric, 2) as avg_goals_per_match,
        ms.total_goals
    from leagues l
    left join countries c  on l.country_id = c.country_id
    left join match_stats ms on l.league_name = ms.league_name
)

select * from joined