{{ simple_cte([
    ('gitlab_dotcom_xmau_metrics', 'gitlab_dotcom_xmau_metrics_common')
]) }},

mapping_table AS (

  SELECT *
  FROM gitlab_dotcom_xmau_metrics

)

{{ dbt_audit(
    cte_ref="mapping_table",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2022-04-09",
    updated_date="2022-05-11"
) }}
