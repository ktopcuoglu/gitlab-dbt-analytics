{{ config({
        "schema": "restricted_safe_common_mart_sales"
    })
}}
WITH fct_retention AS (

    SELECT
    {{
          dbt_utils.star(
            from=ref('fct_retention_parent_account'),
            except=['CREATED_BY',
            'UPDATED_BY',
            'MODEL_CREATED_DATE',
            'MODEL_UPDATED_DATE',
            'DBT_UPDATED_AT',
            'DBT_CREATED_AT'])
    }}
    FROM {{ ref('fct_retention_parent_account') }}

)

{{ dbt_audit(
    cte_ref="fct_retention",
    created_by="@paul_armstrong",
    updated_by="@paul_armstrong",
    created_date="2021-06-03",
    updated_date="2021-06-03"
) }}