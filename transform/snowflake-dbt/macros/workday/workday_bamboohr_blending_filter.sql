{%- macro workday_bamboohr_blending_filter(cte,unique_columns) -%}

SELECT 
  *,
  {{ dbt_utils.surrogate_key(unique_columns) }} AS unique_key,
  IFF(IFF(uploaded_at >= '2022-04-01' ,'workday','bamboohr' ) = source_system,1,0) AS sort_order
FROM {{ cte }} 
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_key ORDER BY sort_order DESC) = 1

{%- endmacro -%}
