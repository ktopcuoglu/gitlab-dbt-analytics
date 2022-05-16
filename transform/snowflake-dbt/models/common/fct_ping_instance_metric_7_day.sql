{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "incremental",
    unique_key = "ping_instance_metric_id"
) }}

{{ simple_cte([
    ('fct_ping_instance_metric', 'fct_ping_instance_metric')
    ])

}},

final AS (

    SELECT
        {{ dbt_utils.star(from=ref('fct_ping_instance_metric'), except=['CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE', 'MODEL_UPDATED_DATE', 'DBT_CREATED_AT', 'DBT_UPDATED_AT']) }}
    FROM fct_ping_instance_metric
    WHERE time_frame = '7d'
    {% if is_incremental() %}
      
      AND ping_created_at >= (SELECT MAX(ping_created_at) FROM {{this}})
    
    {% endif %}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@icooper-acp",
    updated_by="@iweeks",
    created_date="2022-05-03",
    updated_date="2022-05-12"
) }}
