{{ config(
    materialized='ephemeral'
) }}

with source AS (
  SELECT
    *
  FROM {{ ref('dim_issue') }} 
  WHERE ultimate_parent_namespace_id IN (6543, 9970) -- 531 vs 521,491 vs 83,656,512
    AND ARRAY_CONTAINS('infradev'::VARIANT, labels)
)

SELECT *
FROM source