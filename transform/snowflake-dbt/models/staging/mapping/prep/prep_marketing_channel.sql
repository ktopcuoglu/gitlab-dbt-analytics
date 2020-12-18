{{config({
    "materialized": "table",
    "schema": "common_mapping"
  })
}}

{{ generate_single_field_dimension (
    model_name="map_marketing_channel",
    id_column="marketing_channel_name",
    id_column_name="dim_marketing_channel_id",
    dimension_column="marketing_channel_name",
    dimension_column_name="marketing_channel_name",
) }}

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2020-12-18",
    updated_date="2020-12-18"
) }}
