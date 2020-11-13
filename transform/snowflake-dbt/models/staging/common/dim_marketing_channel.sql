{{config({
    "materialized": "table",
    "schema": "common"
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
    created_by="@paul_armstrong",
    updated_by="@paul_armstrong",
    created_date="2020-10-26",
    updated_date="2020-10-26"
) }}
