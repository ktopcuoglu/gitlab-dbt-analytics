{{ config(
    tags=["product", "mnpi_exception"]
) }}

WITH fct_ping_instance_metric_rolling_6_months AS (

    SELECT
      {{ dbt_utils.star(from=ref('fct_ping_instance_metric_rolling_13_months'), except=['CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE', 'MODEL_UPDATED_DATE', 'DBT_CREATED_AT', 'DBT_UPDATED_AT']) }}
    FROM {{ ref('fct_ping_instance_metric_rolling_13_months') }} 
    WHERE DATE_TRUNC(MONTH, fct_ping_instance_metric_rolling_13_months.ping_created_date) >= DATEADD(MONTH, -6, DATE_TRUNC(MONTH,CURRENT_DATE))

)

{{ dbt_audit(
    cte_ref="fct_ping_instance_metric_rolling_6_months",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2022-07-20",
    updated_date="2022-07-29"
) }}