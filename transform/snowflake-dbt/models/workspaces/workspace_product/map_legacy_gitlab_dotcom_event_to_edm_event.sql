SELECT 
  dotcom_event_name::VARCHAR AS dotcom_event_name, 
  prep_event_name::VARCHAR AS prep_event_name
FROM {{ ref('legacy_gitlab_dotcom_event_to_edm_event') }}