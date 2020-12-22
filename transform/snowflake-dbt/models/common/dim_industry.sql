{{ generate_single_field_dimension(model_name="prep_sfdc_account",
                                   id_column="dim_industry_name_source",
                                   id_column_name="dim_industry_id",
                                   dimension_column="dim_industry_name_source",
                                   dimension_column_name="industry_name",
                                   where_clause=None)
}}


{{ dbt_audit(
    cte_ref="unioned",
    created_by="@paul_armstrong",
    updated_by="@paul_armstrong",
    created_date="2020-10-26",
    updated_date="2020-12-10"
) }}
