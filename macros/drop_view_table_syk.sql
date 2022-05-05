{% macro drop_view_table_syk(to_be_droppet,type='view') %}

{% if type=='view' %}

 drop view {{to_be_droppet}}

{% endif %}
{% endmacro %}