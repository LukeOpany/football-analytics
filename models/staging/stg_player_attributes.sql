with source as (
    select * from {{ source('raw', 'player_attributes') }}
),

renamed as (
    select
        id                      as player_attr_id,
        player_api_id,
        player_fifa_api_id,
        date                    ::date as snapshot_date,
        overall_rating,
        potential,
        preferred_foot,
        attacking_work_rate,
        defensive_work_rate,
        crossing,
        finishing,
        heading_accuracy,
        short_passing,
        dribbling,
        curve,
        free_kick_accuracy,
        long_passing,
        ball_control,
        acceleration,
        sprint_speed,
        agility,
        reactions,
        balance,
        shot_power,
        jumping,
        stamina,
        strength,
        long_shots,
        aggression,
        interceptions,
        positioning,
        vision,
        penalties,
        marking,
        sliding_tackle,
        standing_tackle,
        volleys,
        gk_diving,
        gk_handling,
        gk_kicking,
        gk_positioning,
        gk_reflexes
    from source
)

select * from renamed