WITH source AS (

    SELECT *
    FROM {{ ref('zengrc_issue_source') }}

)

SELECT
  issue_id,
  issue_code,
  issue_created_at,
  issue_description,
  issue_notes,
  issue_status,
  issue_stop_date,
  issue_tags,
  issue_title,
  zengrc_object_type,
  issue_updated_at,
  issue_loaded_at,
  remediation_recommendations,
  deficiency_range,
  risk_rating,
  observation_issue_owner,
  likelihood,
  impact,
  gitlab_issue_url,
  gitlab_assignee,
  department,
  type_of_deficiency,
  internal_control_component,
  severity_of_deficiency,
  financial_system_line_item,
  is_reported_to_audit_committee,
  deficiency_theme,
  remediated_evidence,
  financial_assertion_affected_by_deficiency,
  compensating_controls
FROM source