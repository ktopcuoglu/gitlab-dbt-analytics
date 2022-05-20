{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}


WITH source AS (

    SELECT *
    FROM {{ref('usage_ping_metrics_source')}}

), final AS (

    SELECT
      {{ dbt_utils.surrogate_key(['metrics_path']) }}                         AS ping_metric_id,
      metrics_path                                                            AS metrics_path,
      'raw_usage_data_payload['''
        || REPLACE(metrics_path, '.', '''][''')
        || ''']'                                                              AS sql_friendly_path,
      data_source                                                             AS data_source,
      description                                                             AS description,
      product_category                                                        AS product_category,
      IFF(substring(product_group, 0, 5) = 'group',
        SPLIT_PART(REPLACE(product_group, ' ', '_'), ':', 3),
        REPLACE(product_group, ' ', '_'))                                     AS group_name,
      product_section                                                         AS section_name,
      IFF(substring(product_stage, 0, 7) = 'devops:',
        SPLIT_PART(REPLACE(product_stage, ' ', '_'), ':', 3),
        REPLACE(product_stage, ' ', '_'))                                     AS stage_name,
      milestone                                                               AS milestone,
      skip_validation                                                         AS skip_validation,
      metrics_status                                                          AS metrics_status,
      tier                                                                    AS tier,
      time_frame                                                              AS time_frame,
      value_type                                                              AS value_type,
      IFNULL(is_gmau, FALSE)                                                  AS is_gmau,
      IFNULL(is_smau, FALSE)                                                  AS is_smau,
      IFNULL(is_paid_gmau, FALSE)                                             AS is_paid_gmau,
      IFNULL(is_umau, FALSE)                                                  AS is_umau,
      snapshot_date                                                           AS snapshot_date,
      uploaded_at                                                             AS uploaded_at
    FROM source
      QUALIFY MAX(uploaded_at) OVER() = uploaded_at

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@icooper-acp",
    updated_by="@chrissharp",
    created_date="2022-04-14",
    updated_date="2022-05-17"
) }}
