{{config({
    "schema": "common"
  })
}}

{{ generate_single_field_dimension_from_prep (
    model_name="prep_sfdc_account",
    dimension_column="dim_sales_segment_name_source",
) }}


{{ dbt_audit(
    cte_ref="unioned",
    created_by="@msendal",
    updated_by="@msendal",
    created_date="2020-11-05",
    updated_date="2020-11-05"
) }}
