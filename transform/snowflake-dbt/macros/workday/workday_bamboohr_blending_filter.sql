{%- macro workday_bamboohr_blending_filter(cte,unique_columns,sort_columns=1) -%}

SELECT 
  *,
  {{ dbt_utils.surrogate_key(unique_columns) }} AS unique_filter_key,
  IFF(IFF(uploaded_at >= '2022-04-01' ,'workday','bamboohr' ) = source_system,1,0) AS sort_order
FROM {{ cte }} 
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_filter_key ORDER BY sort_order DESC, {{ sort_columns }}) = 1

{%- endmacro -%}
