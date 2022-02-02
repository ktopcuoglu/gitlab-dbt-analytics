
WITH source AS (

  SELECT {{ nohash_sensitive_columns('edcast_glue_groups_g3_group_performance_data_explorer','email') }}
  FROM {{ref('edcast_glue_groups_g3_group_performance_data_explorer')}}

)

SELECT *
FROM source
