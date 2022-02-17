{%macro test_unique_values_syk(model, column_name) %}
select 
    {{column_name}} AS unique_field,
    count(*) AS "n_records" 
    FROM {{model}}
where {{column_name}} is not null
group by {{column_name}}
having count(*) > 1
{% endmacro %}