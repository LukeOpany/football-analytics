select
    player_name,
    stat_year,
    avg_overall_rating,
    avg_potential,
    -- how close are they to their ceiling?
    round(
        (avg_overall_rating / nullif(avg_potential, 0)) * 100
    , 1)                                as pct_of_potential_reached,
    avg_potential - avg_overall_rating  as room_to_grow
from {{ ref('fct_player_stats') }}
where avg_overall_rating is not null
  and stat_year = 2015   -- change this to any year you want
order by avg_overall_rating desc
limit 50