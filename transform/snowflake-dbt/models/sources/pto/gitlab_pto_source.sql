WITH source AS (

    SELECT *
    FROM {{ source('pto', 'gitlab_pto')}}

), deduped AS (

    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (PARTITION BY jsontext['uuid']::VARCHAR ORDER BY uploaded_at DESC) = 1
    
), each_pto_day AS (

    SELECT
      jsontext['end_date']::DATE                            AS end_date,
      jsontext['start_date']::DATE                          AS start_date,
      jsontext['status']::VARCHAR                           AS pto_status,
      jsontext['team_member']['day_length_hours']::NUMBER   AS employee_day_length,
      jsontext['team_member']['department']::VARCHAR        AS employee_department,
      jsontext['team_member']['division']::VARCHAR          AS employee_division,
      jsontext['team_member']['hris_id']::NUMBER            AS hr_employee_id,
      jsontext['team_member']['uuid']::VARCHAR              AS employee_uuid,
      jsontext['uuid']::VARCHAR                             AS pto_uuid,
      ooo_days.value['date']::DATE                          AS pto_date,
      ooo_days.value['end_time']::TIMESTAMP                 AS pto_ends_at,
      ooo_days.value['is_holiday']::BOOLEAN                 AS is_holiday,
      ooo_days.value['recorded_hours']::NUMBER              AS recorded_hours,
      ooo_days.value['start_time']::TIMESTAMP               AS pto_starts_at,
      ooo_days.value['total_hours']::NUMBER                 AS total_hours
    FROM deduped,
    LATERAL FLATTEN(INPUT => jsontext['ooo_days']::ARRAY) ooo_days

)
SELECT *
FROM each_pto_day