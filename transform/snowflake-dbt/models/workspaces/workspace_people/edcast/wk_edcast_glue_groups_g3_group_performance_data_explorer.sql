
WITH source AS (

  SELECT {{ hash_sensitive_columns('edcast_glue_groups_g3_group_performance_data_explorer') }}
  FROM {{ref('edcast_glue_groups_g3_group_performance_data_explorer')}}

)

SELECT *
FROM source
