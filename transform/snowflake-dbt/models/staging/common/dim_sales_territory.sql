{{config({
    "materialized": "table",
    "schema": "common"
  })
}}

{{ generate_single_field_dimension (
    model_name="prep_sfdc_account",
    id_column="tsp_territory",
    id_column_name="dim_sales_territory_id",
    dimension_column="tsp_territory",
    dimension_column_name="sales_territory_name"
) }}


{{ dbt_audit(
    cte_ref="unioned",
    created_by="@paul_armstrong",
    updated_by="@paul_armstrong",
    created_date="2020-10-26",
    updated_date="2020-10-26"
) }}
