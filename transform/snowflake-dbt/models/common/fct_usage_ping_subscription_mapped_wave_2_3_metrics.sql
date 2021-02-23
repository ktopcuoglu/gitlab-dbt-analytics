WITH monthly AS (

    SELECT *
    FROM {{ ref('prep_usage_ping_subscription_mapped_wave2_3_metrics') }}
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY
        dim_subscription_id,
        uuid,
        hostname,
        ping_created_at_month
      ORDER BY ping_created_at DESC
      ) = 1

)

{{ dbt_audit(
    cte_ref="monthly",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-02-08",
    updated_date="2021-02-08"
) }}