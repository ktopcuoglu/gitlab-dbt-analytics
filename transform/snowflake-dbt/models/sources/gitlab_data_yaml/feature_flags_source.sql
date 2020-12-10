WITH filtered_attributes AS (

    SELECT
      jsontext['name']::VARCHAR            AS name,
      jsontext['type']::VARCHAR            AS type,
      jsontext['milestone']::VARCHAR       AS milestone,
      jsontext['default_enabled']::BOOLEAN AS default_enabled,
      jsontext['group']::VARCHAR           AS group
    FROM  {{ source('gitlab_feature_flags_yaml', 'feature_flags') }}
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY jsontext['group'] ORDER BY uploaded_at DESC
    ) = 1

)

SELECT * 
FROM filtered_attributes
 