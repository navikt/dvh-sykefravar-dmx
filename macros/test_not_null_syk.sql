{% macro test_not_null_syk(model,column_name) %}

select count(*)
from {{ model }}
where {{ column_name }} is null

{% endmacro %}