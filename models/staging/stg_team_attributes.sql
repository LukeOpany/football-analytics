with source as (
    select * from {{ source('raw', 'team_attributes') }}
),

renamed as (
    select
        id                              as team_attr_id,
        team_api_id,
        team_fifa_api_id,
        date                            ::date as snapshot_date,
        -- Build up play
        "buildUpPlaySpeed"              as build_up_play_speed,
        "buildUpPlaySpeedClass"         as build_up_play_speed_class,
        "buildUpPlayDribbling"          as build_up_play_dribbling,
        "buildUpPlayDribblingClass"     as build_up_play_dribbling_class,
        "buildUpPlayPassing"            as build_up_play_passing,
        "buildUpPlayPassingClass"       as build_up_play_passing_class,
        "buildUpPlayPositioningClass"   as build_up_play_positioning_class,
        -- Chance creation
        "chanceCreationPassing"         as chance_creation_passing,
        "chanceCreationPassingClass"    as chance_creation_passing_class,
        "chanceCreationCrossing"        as chance_creation_crossing,
        "chanceCreationCrossingClass"   as chance_creation_crossing_class,
        "chanceCreationShooting"        as chance_creation_shooting,
        "chanceCreationShootingClass"   as chance_creation_shooting_class,
        "chanceCreationPositioningClass" as chance_creation_positioning_class,
        -- Defence
        "defencePressure"               as defence_pressure,
        "defencePressureClass"          as defence_pressure_class,
        "defenceAggression"             as defence_aggression,
        "defenceAggressionClass"        as defence_aggression_class,
        "defenceTeamWidth"              as defence_team_width,
        "defenceTeamWidthClass"         as defence_team_width_class,
        "defenceDefenderLineClass"      as defence_defender_line_class
    from source
)

select * from renamed