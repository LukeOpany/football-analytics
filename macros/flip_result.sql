{% macro flip_result(result_column) %}

case
    when {{ result_column }} = 'win'  then 'loss'
    when {{ result_column }} = 'loss' then 'win'
    else 'draw'
end

{% endmacro %}