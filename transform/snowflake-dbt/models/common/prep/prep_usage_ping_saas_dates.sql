WITH base AS (
  
   SELECT * 
   FROM PROD.legacy.dim_usage_pings
   WHERE ping_source = 'SaaS'

), saas_pings AS (
  
    SELECT 
      id                                                                AS dim_usage_ping_id, 
      DATE_TRUNC('DAY', created_at)                                     AS ping_created_at_date,
      DATE_TRUNC('DAY', DATEADD('days', -28, created_at))               AS ping_created_at_28_days_earlier,
      DATE_TRUNC('YEAR', created_at)                                    AS ping_created_at_year,
      DATE_TRUNC('MONTH', created_at)                                   AS ping_created_at_month,
      DATE_TRUNC('WEEK', created_at)                                    AS ping_created_at_week
    FROM base 

), final AS (

    SELECT *
    FROM saas_pings
    QUALIFY row_number() OVER (PARTITION BY ping_created_at_date ORDER BY dim_usage_ping_id DESC) = 1

) 

{{ dbt_audit(
    cte_ref="final",
    created_by="@kathleentam",
    updated_by="@kathleentam",
    created_date="2021-01-11",
    updated_date="2021-01-11"
) }}
