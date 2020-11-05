{{config({
    "materialized": "table",
    "schema": "common"
  })
}}

{{ generate_single_field_dimension (
    model_name="sfdc_opportunity_source",
    id_column="sales_qualified_source",
    id_column_name="dim_opportunity_source_id",
    dimension_column="sales_qualified_source",
    dimension_column_name="opportunity_source_name",
    where_clause="NOT is_deleted"
) }}

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@paul_armstrong",
    updated_by="@paul_armstrong",
    created_date="2020-10-26",
    updated_date="2020-10-26"
) }}
