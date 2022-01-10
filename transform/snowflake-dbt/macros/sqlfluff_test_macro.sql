{% macro sqlfluff_test_macro(columns) %}
COALESCE({{columns}}, MD5(-1))
{% endmacro %}