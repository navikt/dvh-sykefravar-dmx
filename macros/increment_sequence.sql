{%- macro increment_sequence() -%}

  {{ this.name }}_seq.nextval

{%- endmacro -%}