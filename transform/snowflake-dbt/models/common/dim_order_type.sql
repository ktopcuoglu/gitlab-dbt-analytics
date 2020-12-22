

{{ generate_single_field_dimension (
    model_name="sfdc_opportunity_source",
    id_column="order_type_stamped",
    id_column_name="dim_order_type_id",
    dimension_column="order_type_stamped",
    dimension_column_name="order_type_name",
    where_clause="NOT is_deleted"
) }}

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@paul_armstrong",
    updated_by="@mcooperDD",
    created_date="2020-11-02",
    updated_date="2020-12-10"
) }}
