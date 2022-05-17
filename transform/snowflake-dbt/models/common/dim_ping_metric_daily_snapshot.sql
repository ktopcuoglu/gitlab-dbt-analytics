{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}


WITH source AS (

    SELECT *
    FROM {{ source('gitlab_data_yaml', 'usage_ping_metrics') }}

), snapshot_dates AS (

   SELECT *
   FROM {{ ref('dim_date') }}
   WHERE date_actual >= '2020-03-01' and date_actual <= CURRENT_DATE

), intermediate AS (

    SELECT
      d.value                                 AS data_by_row,
      uploaded_at                             AS uploaded_at,
      date_trunc('day', uploaded_at)::DATE    AS snapshot_date
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => TRUE) d
    QUALIFY ROW_NUMBER() OVER (PARTITION BY data_by_row['key_path'], uploaded_at::DATE ORDER BY uploaded_at DESC) = 1

), renamed AS (

     SELECT
      data_by_row['key_path']::TEXT                                                     AS metrics_path,
      data_by_row['data_source']::TEXT                                                  AS data_source,
      data_by_row['description']::TEXT                                                  AS description,
      data_by_row['product_category']::TEXT                                             AS product_category,
      data_by_row['product_group']::TEXT                                                AS product_group,
      data_by_row['product_section']::TEXT                                              AS product_section,
      data_by_row['product_stage']::TEXT                                                AS product_stage,
      data_by_row['milestone']::TEXT                                                    AS milestone,
      data_by_row['skip_validation']::TEXT                                              AS skip_validation,
      data_by_row['status']::TEXT                                                       AS metrics_status,
      data_by_row['tier']                                                               AS tier,
      data_by_row['time_frame']::TEXT                                                   AS time_frame,
      data_by_row['value_type']::TEXT                                                   AS value_type,
      ARRAY_CONTAINS( 'gmau'::VARIANT , data_by_row['performance_indicator_type'])      AS is_gmau,
      ARRAY_CONTAINS( 'smau'::VARIANT , data_by_row['performance_indicator_type'])      AS is_smau,
      ARRAY_CONTAINS( 'paid_gmau'::VARIANT , data_by_row['performance_indicator_type']) AS is_paid_gmau,
      ARRAY_CONTAINS( 'umau'::VARIANT , data_by_row['performance_indicator_type'])      AS is_umau,
      uploaded_at,
      snapshot_date
    FROM intermediate

), ping_metric_hist AS (

    SELECT
      metrics_path                                                                      AS metrics_path,
      'raw_usage_data_payload['''
        || REPLACE(metrics_path, '.', '''][''')
        || ''']'                                                                        AS sql_friendly_path,
      data_source                                                                       AS data_source,
      description                                                                       AS description,
      product_category                                                                  AS product_category,
      IFF(substring(product_group, 0, 5) = 'group', 
        SPLIT_PART(REPLACE(product_group, ' ', '_'), ':', 3), 
        REPLACE(product_group, ' ', '_'))                                               AS group_name,
      product_section                                                                   AS section_name,
      product_stage                                                                     AS stage_name,
      milestone                                                                         AS milestone,
      skip_validation                                                                   AS skip_validation,
      metrics_status                                                                    AS metrics_status,
      tier                                                                              AS tier,
      time_frame                                                                        AS time_frame,
      value_type                                                                        AS value_type,
      is_gmau                                                                           AS is_gmau,
      is_smau                                                                           AS is_smau,
      is_paid_gmau                                                                      AS is_paid_gmau,
      is_umau                                                                           AS is_umau,
      uploaded_at                                                                       AS uploaded_at,
      snapshot_date                                                                     AS snapshot_date
    FROM renamed

), ping_metric_spined AS (

    SELECT
      {{ dbt_utils.surrogate_key(['metrics_path', 'snapshot_dates.date_id']) }}         AS ping_metric_hist_id,
      snapshot_dates.date_id                                                            AS snapshot_id,
      ping_metric_hist.metrics_path,
      ping_metric_hist.sql_friendly_path,
      ping_metric_hist.data_source,
      ping_metric_hist.description,
      ping_metric_hist.product_category,
      ping_metric_hist.group_name,
      ping_metric_hist.section_name,
      ping_metric_hist.stage_name,
      ping_metric_hist.milestone,
      ping_metric_hist.skip_validation,
      ping_metric_hist.metrics_status,
      ping_metric_hist.tier,
      ping_metric_hist.time_frame,
      ping_metric_hist.value_type,
      ping_metric_hist.is_gmau,
      ping_metric_hist.is_smau,
      ping_metric_hist.is_paid_gmau,
      ping_metric_hist.is_umau,
      uploaded_at
    FROM ping_metric_hist
    INNER JOIN snapshot_dates
      ON snapshot_dates.date_actual = ping_metric_hist.snapshot_date

)

{{ dbt_audit(
    cte_ref="ping_metric_spined",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2022-05-13",
    updated_date="2022-05-17"
) }}
