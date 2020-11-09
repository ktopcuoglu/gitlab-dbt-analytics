WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_clari_ai_projection_source') }}

)

{{ dbt_audit(
    cte_ref="source",
    created_by="@paul_armstrong",
    updated_by="@paul_armstrong",
    created_date="2020-11-09",
    updated_date="2020-11-09"
) }}
