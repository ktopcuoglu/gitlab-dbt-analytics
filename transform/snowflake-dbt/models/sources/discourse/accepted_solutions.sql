WITH source AS (

    SELECT *
    FROM {{ source('discourse', 'accepted_solutions') }}

), parsed AS (

    SELECT
      json_value.value['start_date']::datetime  as report_start_date,
      json_value.value['title']::varchar        as report_title,
      json_value.value['type']::varchar         as report_type,
      json_value.value['total']::varchar        as report_total,
      data_level_one.value['x']::DATE           AS report_value_date,
      data_level_one.value['y']::INT            AS report_value,
      uploaded_at                               AS uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext), outer => true) json_value,
    lateral flatten(json_value.value:data,'') data_level_one

), dedupe AS (

    SELECT DISTINCT
      report_start_date,
      report_title,
      report_type,
      request_type,
      request_label,
      report_value_date,
      report_value,
      max(uploaded_at)      AS last_uploaded_at
    FROM parsed
    GROUP BY
      report_start_date,
      report_title,
      report_type,
      request_type,
      request_label,
      report_value_date,
      report_value
)

SELECT *
FROM dedupe
