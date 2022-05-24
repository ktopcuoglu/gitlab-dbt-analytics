{% macro backup_to_gcs() %}

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

                {% for table in tables %}
                    {% if table.name not in ['FCT_MRR_SNAPSHOT', 'MART_ARR_SNAPSHOT', 'GITLAB_DOTCOM_PROJECT_STATISTICS_SNAPSHOTS', 'MART_WATERFALL_SNAPSHOT', 'MART_ARR_SNAPSHOT_20210609', 'GITLAB_DOTCOM_ISSUES_SNAPSHOTS', 'SFDC_ACCOUNT_SNAPSHOTS', 'DIM_SUBSCRIPTION_SNAPSHOT', 'MART_RETENTION_PARENT_ACCOUNT_SNAPSHOT', 'MART_CHARGE_SNAPSHOT', 'FCT_MRR_SNAPSHOT_20210531', 'GITLAB_DOTCOM_NAMESPACE_ROOT_STORAGE_STATISTICS_SNAPSHOTS', 'GITLAB_DOTCOM_PROJECTS_SNAPSHOTS', 'GITLAB_DOTCOM_NAMESPACES_SNAPSHOTS', 'GITLAB_DOTCOM_MEMBERS_SNAPSHOTS', 'MART_ARR_SNAPSHOT_20210531', 'GITLAB_DOTCOM_NAMESPACE_STATISTICS_SNAPSHOTS', 'SFDC_OPPORTUNITY_SNAPSHOTS','GITLAB_DOTCOM_GITLAB_SUBSCRIPTIONS_NAMESPACE_ID_SNAPSHOTS','MART_AVAILABLE_TO_RENEW_SNAPSHOT'] %}
                        {{ log('Backing up ' ~ table.name ~ '...', info = true) }}
                        {% set backup_table_command = get_backup_table_command(table, day_of_month) %}
                        {{ backup_table_command }}
                    {% endif %}
                {% endfor %}

            {% endfor %}

        {% endfor %}

{%- endmacro -%}
