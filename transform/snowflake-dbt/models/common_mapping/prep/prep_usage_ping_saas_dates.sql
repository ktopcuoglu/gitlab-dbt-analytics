WITH base AS (
  
   SELECT * 
   FROM {{ ref('prep_usage_ping') }}
   WHERE ping_source = 'SaaS'

), saas_pings AS (
  
    SELECT 
      dim_usage_ping_id, 
      ping_created_at_date,
      ping_created_at_28_days_earlier,
      ping_created_at_year,
      ping_created_at_month,
      ping_created_at_week
    FROM base 

), final AS (

    SELECT *
    FROM saas_pings
    QUALIFY row_number() OVER (PARTITION BY ping_created_at_date ORDER BY dim_usage_ping_id DESC) = 1

) 

{{ dbt_audit(
    cte_ref="final",
    created_by="@kathleentam",
    updated_by="@ischweickartDD",
    created_date="2021-01-11",
    updated_date="2021-04-05"
) }}