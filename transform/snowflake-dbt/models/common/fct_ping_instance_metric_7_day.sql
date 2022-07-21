{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{ simple_cte([
    ('fct_ping_instance_metric_rolling_24_months', 'fct_ping_instance_metric_rolling_24_months')
    ])

}}

, final AS (

    SELECT
        {{ dbt_utils.star(from=ref('fct_ping_instance_metric_rolling_24_months'), except=['CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE', 'MODEL_UPDATED_DATE', 'DBT_CREATED_AT', 'DBT_UPDATED_AT']) }}
    FROM fct_ping_instance_metric_rolling_24_months
    WHERE time_frame = '7d'

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@icooper-acp",
    updated_by="@iweeks",
    created_date="2022-05-03",
    updated_date="2022-07-20"
) }}
