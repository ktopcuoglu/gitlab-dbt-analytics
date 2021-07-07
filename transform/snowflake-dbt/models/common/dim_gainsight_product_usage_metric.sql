WITH gainsight_metrics AS (

    SELECT
      wave,
      metric_name
    FROM {{ ref('gainsight_wave_2_3_metrics') }}

)

{{ dbt_audit(
    cte_ref="gainsight_metrics",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-07-07",
    updated_date="2021-07-07"
) }}