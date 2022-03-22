{{ config(
    tags=["product", "mnpi_exception"],
    full_refresh = false,
    materialized = "incremental",
    unique_key = "prep_service_ping_instance_flattened_id"
) }}


WITH source AS (

      SELECT
          *
      FROM {{ ref('version_raw_usage_data_source')}} as usage
      {% if is_incremental() %}

        WHERE created_at >= (SELECT COALESCE(MAX(created_at), '2000-01-01') FROM {{this}})

    {% endif %}

) , flattened_high_level as (

  SELECT
      {{ dbt_utils.surrogate_key(['raw_usage_data_id', 'path']) }}      AS prep_service_ping_instance_flattened_id,
      raw_usage_data_id,
      created_at,
      path                                                              AS metrics_path,
      value                                                             AS metric_value,
      raw_usage_data_payload
    FROM source,
    LATERAL FLATTEN(INPUT => raw_usage_data_payload,
    RECURSIVE => TRUE)

)

  {{ dbt_audit(
      cte_ref="flattened_high_level",
      created_by="@icooper-acp",
      updated_by="@icooper-acp",
      created_date="2022-03-17",
      updated_date="2022-03-17"
  ) }}
