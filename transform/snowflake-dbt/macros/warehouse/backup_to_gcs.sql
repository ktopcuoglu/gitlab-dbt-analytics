{% macro backup_to_gcs() %}

    {%- call statement('backup', fetch_result=true, auto_begin=true) -%}

        {% set backups =
            {
                "RAW":
                    [
                        "SNAPSHOTS"
                    ]
            }
        %}

        {% set day_of_month = run_started_at.strftime("%d") %}
        
        {{ log('Backing up for Day ' ~ day_of_month, info = true) }}

        {% for database, schemas in backups.items() %}
        
            {% for schema in schemas %}
        
                {{ log('Getting tables in schema ' ~ schema ~ '...', info = true) }}

                {% set tables = dbt_utils.get_relations_by_prefix(schema.upper(), '', exclude='FIVETRAN_%', database=database) %}
                {{ log('TABLES: ' ~ tables ~ '...', info = true) }}
                {% for table in tables %}
                    {{ log('Backing up ' ~ table.name ~ '...', info = true) }}
                    {% set backup_table_command = get_backup_table_command(table, day_of_month) %}
                    {{ backup_table_command }}
                    {{ log('TEST END: Backing up ' ~ table.name ~ '...', info = true) }}
                {% endfor %}
                {{ log('TEST END FOR1') }}
            {% endfor %}
            {{ log('TEST END FOR2') }}
        {% endfor %}
        {{ log('TEST END FOR3') }}
    {%- endcall -%}

{%- endmacro -%}
