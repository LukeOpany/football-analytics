with player_attrs as (
    select * from {{ ref('stg_player_attributes') }}
),

players as (
    select * from {{ ref('stg_players') }}
),

-- One row per player per year (average of all snapshots that year)
yearly_stats as (
    select
        player_api_id,
        extract(year from snapshot_date)    as stat_year,
        round(avg(overall_rating)::numeric, 1) as avg_overall_rating,
        round(avg(potential)::numeric, 1)      as avg_potential,
        round(avg(finishing)::numeric, 1)      as avg_finishing,
        round(avg(dribbling)::numeric, 1)      as avg_dribbling,
        round(avg(short_passing)::numeric, 1)  as avg_short_passing,
        round(avg(sprint_speed)::numeric, 1)   as avg_sprint_speed,
        round(avg(stamina)::numeric, 1)        as avg_stamina,
        round(avg(strength)::numeric, 1)       as avg_strength,
        count(*)                               as snapshot_count
    from player_attrs
    group by player_api_id, extract(year from snapshot_date)
),

joined as (
    select
        p.player_id,
        p.player_name,
        ys.player_api_id,
        ys.stat_year,
        ys.avg_overall_rating,
        ys.avg_potential,
        ys.avg_finishing,
        ys.avg_dribbling,
        ys.avg_short_passing,
        ys.avg_sprint_speed,
        ys.avg_stamina,
        ys.avg_strength,
        ys.snapshot_count
    from yearly_stats ys
    left join players p on ys.player_api_id = p.player_api_id
)

select * from joined