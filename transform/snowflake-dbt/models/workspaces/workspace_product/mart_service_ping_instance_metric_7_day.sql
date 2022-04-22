{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "incremental",
    unique_key = "mart_service_ping_instance_metric_id"
) }}

{{ simple_cte([
    ('mart_service_ping_instance_metric', 'mart_service_ping_instance_metric')
    ])

}}

, final AS (

  SELECT
      {{ dbt_utils.star(from=ref('mart_service_ping_instance_metric'), except=['CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE', 'MODEL_UPDATED_DATE', 'DBT_CREATED_AT', 'DBT_UPDATED_AT']) }}
  FROM mart_service_ping_instance_metric
    WHERE time_frame = '7d'
  {% if is_incremental() %}
              AND ping_created_at >= (SELECT MAX(ping_created_at) FROM {{this}})
  {% endif %}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@icooper-acp",
    updated_by="@icooper-acp",
    created_date="2022-04-08",
    updated_date="2022-04-08"
) }}
