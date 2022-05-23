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

                {% for table in tables %}

                    {% if loop.last %}

                        {{ log('THIS IS LAST ONE', info = true) }}

                    {% else %}

                        {{ log('THIS IS NOT THE LAST ONE', info = true) }}

                    {% endif %}

                {% endfor %}

            {% endfor %}

        {% endfor %}

    {%- endcall -%}

{%- endmacro -%}
