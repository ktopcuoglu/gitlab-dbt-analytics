{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ simple_cte([
    ('mart_arr', 'mart_arr')
    ])

}}

-- Determine monthly sub and user count

, subscription_info AS (

  SELECT
    {{ dbt_utils.surrogate_key(['arr_month']) }}          AS rpt_service_ping_instance_subcription_opt_in_monthly_id,
    arr_month                                             AS arr_month,
    SUM(arr)                                              AS arr,
    SUM(quantity)                                         AS total_licensed_users,
    COUNT(DISTINCT dim_subscription_id)                   AS total_subscription_count
  FROM mart_arr
  WHERE product_tier_name != 'Storage'
    AND product_delivery_type = 'Self-Managed'
  GROUP BY 1,2
    ORDER BY 2 DESC

)
 {{ dbt_audit(
     cte_ref="subscription_info",
     created_by="@icooper-acp",
     updated_by="@icooper-acp",
     created_date="2022-04-07",
     updated_date="2022-04-15"
 ) }}
