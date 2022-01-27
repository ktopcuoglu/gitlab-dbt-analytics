
WITH source AS (

  SELECT {{ hash_sensitive_columns('edcast_sheetload_gitlab_certification_tracking_dashboard') }}
  FROM {{ref('edcast_sheetload_gitlab_certification_tracking_dashboard')}}

)

SELECT *
FROM source
