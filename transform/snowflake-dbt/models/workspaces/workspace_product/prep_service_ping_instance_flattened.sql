{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{ config({
    "materialized": "incremental"
    })
}}

{{ simple_cte([
    ('raw_usage_data', 'version_raw_usage_data_source')
    ])

}}

, flattened_high_level as (
    SELECT
      {{ dbt_utils.surrogate_key(['raw_usage_data_id', 'path']) }}       AS prep_service_pinginstance_flattened__id,
      raw_usage_data.*,
      path                                  AS metrics_path,
      value                                 AS metric_value
  FROM raw_usage_data,
    LATERAL FLATTEN(input => raw_usage_data_payload,
    RECURSIVE => true)
)

{{ dbt_audit(
    cte_ref="flattened_high_level",
    created_by="@icooper-acp",
    updated_by="@icooper-acp",
    created_date="2022-03-17",
    updated_date="2022-03-17"
) }}
