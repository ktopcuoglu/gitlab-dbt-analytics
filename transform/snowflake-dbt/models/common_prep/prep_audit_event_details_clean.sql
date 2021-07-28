WITH non_pii_details AS (

    SELECT
      audit_event_id,
      key_name,
      key_value,
      created_at
    FROM {{ ref('gitlab_dotcom_audit_event_details') }}
    WHERE key_name != 'target_details'

), pii_details AS (

    SELECT 
      audit_event_id,
      key_name,
      key_value_hash AS key_value,
      created_at
    FROM {{ ref('gitlab_dotcom_audit_event_details_pii') }}

), unioned AS (

    SELECT *
    FROM non_pii_details
    UNION ALL
    SELECT *
    FROM pii_details

)

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-06-16",
    updated_date="2021-06-16"
) }}