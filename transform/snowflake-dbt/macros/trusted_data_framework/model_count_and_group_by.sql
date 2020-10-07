{% macro model_count_and_group_by(model_name, group_by_column) %}

with model_data AS (

  SELECT
    CAST({{ group_by_column }} AS DATE) AS grouped_date,
    COUNT(*)                            AS num_rows
  FROM {{ ref(model_name) }}
  GROUP BY CAST({{ group_by_column }} AS DATE)

)

SELECT
    DISTINCT
      db.grouped_date                   AS date_day,
      IFNULL(db.num_rows, 0)            AS rowcount
FROM model_data db

{% endmacro %}