{% macro backup_to_gcs() %}


    {% set tables = dbt_utils.get_relations_by_prefix('SNAPSHOTS', '', exclude='FIVETRAN_%', database='RAW') %}

    {% for table in tables %}
        {{ log('Backing up ' ~ table.name ~ '...', info = true) }}
    {% endfor %}

{%- endmacro -%}
