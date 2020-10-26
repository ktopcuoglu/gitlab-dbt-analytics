-- depends on: {{ ref('sfdc_event_source') }}

{{config({
    "materialized": "table",
    "schema": "common"
  })
}}

{{ generate_single_field_dimension ('sfdc_account_source', 'tsp_territory', 'dim_sales_territory_id', 'tsp_territory', 'sales_territory_name') }}

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@paul_armstrong",
    updated_by="@paul_armstrong",
    created_date="2020-10-26",
    updated_date="2020-10-26"
) }}
