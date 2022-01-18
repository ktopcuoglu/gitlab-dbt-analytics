{{ config(
    tags=["people", "edcast"]
) }}


WITH source AS (

  SELECT {{ nohash_sensitive_columns('edcast_sheetload_gitlab_certification_tracking_dashboard','user') }}
  FROM {{ref('edcast_sheetload_gitlab_certification_tracking_dashboard')}}

)

SELECT *
FROM source
