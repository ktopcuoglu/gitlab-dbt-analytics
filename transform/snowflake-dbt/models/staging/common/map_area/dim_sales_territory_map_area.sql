{{config({
    "materialized": "view",
    "schema": "common"
  })
}}

{{ generate_map_area (
    model_name="sfdc_account_source",
    id_column="tsp_territory",
    id_column_name="dim_sales_territory_id",
    dimension_column="tsp_territory",
    dimension_column_name="dim_sales_territory"
) }}


{{ dbt_audit(
    cte_ref="map_data",
    created_by="@paul_armstrong",
    updated_by="@paul_armstrong",
    created_date="2020-10-26",
    updated_date="2020-10-26"
) }}
