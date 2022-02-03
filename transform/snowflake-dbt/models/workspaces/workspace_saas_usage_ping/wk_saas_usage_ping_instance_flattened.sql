{{ config({
    "materialized": "table"
    })
}}

{{ simple_cte([
    ('saas_usage_ping_instance', 'saas_usage_ping_instance')
]) }}

, flattened AS (

    SELECT
      saas_usage_ping_gitlab_dotcom_id                     AS saas_usage_ping_gitlab_dotcom_id,
      ping_date                                            AS ping_date,
      COALESCE(TRY_PARSE_JSON(path)[0]::TEXT, path::TEXT)  AS metric_path,
      value::TEXT                                          AS metric_value,
      recorded_at                                          AS recorded_at,
      version                                              AS version,
      edition                                              AS edition,
      recording_ce_finished_at                             AS recording_ce_finished_at,
      recording_ee_finished_at                             AS recording_ee_finished_at,
      uuid                                                 AS uuid,
      _uploaded_at                                         AS _uploaded_at
    FROM saas_usage_ping_instance,
    LATERAL FLATTEN(INPUT => run_results,
    RECURSIVE => TRUE) 

)
SELECT *
FROM flattened

