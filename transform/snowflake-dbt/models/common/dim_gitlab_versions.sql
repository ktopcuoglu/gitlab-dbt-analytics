{{config({
    "schema": "legacy"
  })
}}

WITH versions AS (

    SELECT *
    FROM {{ ref('version_versions_source') }}

), calculated AS (

    SELECT
      *,
      SPLIT_PART(version, '.', 1)::NUMBER   AS major_version,
      SPLIT_PART(version, '.', 2)::NUMBER   AS minor_version,
      SPLIT_PART(version, '.', 3)::NUMBER   AS patch_number,
      IFF(patch_number = 0, TRUE, FALSE)    AS is_monthly_release,
      created_at::DATE                      AS created_date,
      updated_at::DATE                      AS updated_date
    FROM versions  

), renamed AS (

    SELECT
      id AS version_id,
      version,
      major_version,
      minor_version,
      patch_number,
      is_monthly_release,
      is_vulnerable,
      created_date,
      updated_date
    FROM calculated  

)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@derekatwood",
    updated_by="@msendal",
    created_date="2020-08-06",
    updated_date="2020-09-17"
) }}

