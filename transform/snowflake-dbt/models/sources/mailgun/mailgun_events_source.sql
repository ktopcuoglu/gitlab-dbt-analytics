WITH source AS (

    SELECT *
    FROM {{ source('mailgun', 'mailgun_events') }}

), intermediate AS (

    SELECT d.value as data_by_row, uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext), outer => true) d

), parsed AS (

    SELECT

      data_by_row['id']::VARCHAR                            AS mailgun_event_id,
      data_by_row['message-id']::VARCHAR                    AS message_id,
      data_by_row['timestamp']::TIMESTAMP                   AS event_timestamp,
      data_by_row['event']::VARCHAR                         AS event_type,
      data_by_row['tags']::VARCHAR                          AS tags,
      data_by_row['delivery-status-code']::VARCHAR          AS delivery_status_code,
      data_by_row['delivery-status-message']::VARCHAR       AS delivery_status_message,
      data_by_row['log-level']::VARCHAR                     AS log_level,
      data_by_row['url']::VARCHAR                           AS url,
      data_by_row['recipient']::VARCHAR                     AS recipient,
      data_by_row['sender']::VARCHAR                        AS sender,
      data_by_row['targets']::VARCHAR                       AS targets,
      data_by_row['subject']::VARCHAR                       AS subject,
      data_by_row['city']::VARCHAR                          AS city,
      data_by_row['region']::VARCHAR                        AS region,
      data_by_row['country']::VARCHAR                       AS country,
      NULLIF(data_by_row['is-routed'], '')::BOOLEAN         AS is_routed,
      NULLIF(data_by_row['is-authenticated'], '')::BOOLEAN  AS is_authenticated,
      NULLIF(data_by_row['is-system-test'], '')::BOOLEAN    AS is_system_test,
      NULLIF(data_by_row['is-test-mode'], '')::BOOLEAN      AS is_test_mode,
      uploaded_at::TIMESTAMP                                AS uploaded_at

    FROM intermediate
)
SELECT * 
FROM parsed


