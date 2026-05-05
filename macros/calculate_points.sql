{% macro calculate_points(result_column) %}

case
    when {{ result_column }} = 'win'  then 3
    when {{ result_column }} = 'draw' then 1
    else 0
end

{% endmacro %}