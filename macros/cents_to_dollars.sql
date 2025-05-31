{% macro cent_to_dollar(col_name, dec_place=2) %}
    round({{col_name}}/100,{{dec_place}})
{%endmacro%}