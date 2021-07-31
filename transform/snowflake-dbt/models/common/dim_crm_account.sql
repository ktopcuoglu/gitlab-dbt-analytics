WITH base AS (

    SELECT *
    FROM {{ ref('prep_crm_account') }}

)

{{ dbt_audit(
    cte_ref="base",
    created_by="@msendal",
    updated_by="@iweeks",
    created_date="2020-06-01",
    updated_date="2021-07-29"
) }}
