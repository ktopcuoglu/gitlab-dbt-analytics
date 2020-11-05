{{config({
    "schema": "common"
  })
}}

{{ generate_single_field_dimension_from_prep (
    model_name="prep_sfdc_account",
    dimension_column="dim_sales_territory_name_source",
) }}


{{ dbt_audit(
    cte_ref="unioned",
    created_by="@paul_armstrong",
    updated_by="@paul_armstrong",
    created_date="2020-10-26",
    updated_date="2020-10-26"
) }}
