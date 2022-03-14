{{ config({
    "alias": "dim_date",
    "post-hook": '{{ apply_dynamic_data_masking(columns = [{"updated_by":"string"},{"created_by":"string"}]) }}'
}) }}

WITH dates AS (

  SELECT *
  FROM {{ ref('date_details') }}

), final AS (

  SELECT
    {{ get_date_id('date_actual') }}                                AS date_id,
    *,
    COUNT(date_id) OVER (PARTITION BY first_day_of_month)           AS days_in_month_count
  FROM dates

)

SELECT *
FROM final
