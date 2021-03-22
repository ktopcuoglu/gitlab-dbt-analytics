WITH date_details AS (
  
    SELECT *
    FROM {{ ref("date_details") }}
    WHERE last_day_of_month = date_actual
     
), application_settings_snapshot AS (

   SELECT
     *,
     IFNULL(valid_to, CURRENT_TIMESTAMP) AS valid_to_
   FROM {{ ref('gitlab_dotcom_application_settings_snapshots_base') }}

), application_settings_snapshot_monthly AS (
  
    SELECT
      DATE_TRUNC('month', date_details.date_actual) AS snapshot_month,
      application_settings_snapshot.application_settings_id,
      application_settings_snapshot.shared_runners_minutes,
      application_settings_snapshot.repository_size_limit
    FROM application_settings_snapshot
    INNER JOIN date_details
      ON date_details.date_actual BETWEEN application_settings_snapshot.valid_from AND application_settings_snapshot.valid_to_
    QUALIFY ROW_NUMBER() OVER(PARTITION BY snapshot_month, application_settings_id ORDER BY valid_to_ DESC) = 1
  
)

SELECT *
FROM application_settings_snapshot_monthly
