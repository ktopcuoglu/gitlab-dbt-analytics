{% macro backup_to_gcs() %}

        {% set day_of_month = run_started_at.strftime("%d") %}

        {{ log('Backing up for Day ' ~ day_of_month, info = true) }}

        {% set tables = dbt_utils.get_relations_by_prefix('SNAPSHOTS', '', exclude='FIVETRAN_%', database='RAW') %}

        {% for table in tables %}
            {{ log('Backing up ' ~ table.name ~ '...', info = true) }}
            {% set backup_table_command = get_backup_table_command(table, day_of_month) %}
            {{ backup_table_command }}
        {% endfor %}

{%- endmacro -%}
