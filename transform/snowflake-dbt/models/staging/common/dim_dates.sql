WITH dates AS (

  SELECT *
  FROM {{ ref('date_details') }}

), final AS (

  SELECT
    TO_NUMBER(TO_CHAR(date_actual,'YYYYMMDD'),'99999999')                           AS date_id,
    *
  FROM dates

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@msendal",
    updated_by="@msendal",
    created_date="2020-06-01",
    updated_date="2020-09-17"
) }}
