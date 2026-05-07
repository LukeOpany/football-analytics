with players as (
    select * from {{ ref('stg_players') }}
),

player_attrs as (
    select * from {{ ref('stg_player_attributes') }}
),

latest_attrs as (
    select
        *,
        row_number() over (
            partition by player_api_id
            order by snapshot_date desc, player_attr_id desc
        ) as attr_rank
    from player_attrs
),

joined as (
    select
        p.player_id,
        p.player_api_id,
        p.player_fifa_api_id,
        p.player_name,
        p.birthday,
        p.height,
        p.weight,
        la.snapshot_date as latest_attribute_date,
        la.overall_rating,
        la.potential,
        la.preferred_foot,
        la.attacking_work_rate,
        la.defensive_work_rate,
        la.finishing,
        la.dribbling,
        la.short_passing,
        la.sprint_speed,
        la.stamina,
        la.strength
    from players p
    left join latest_attrs la
        on p.player_api_id = la.player_api_id
        and la.attr_rank = 1
)

select * from joined
