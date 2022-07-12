WITH source AS (

    SELECT *
    FROM {{ source('mailgun', 'mailgun_events') }}

), intermediate AS (

    SELECT d.value as data_by_row, uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext), outer => true) d

), parsed AS (

    SELECT
      data_by_row:"id"::VARCHAR                                     AS mailgun_event_id,
      data_by_row:"message"."headers"."message-id"::VARCHAR         AS message_id,
      data_by_row:"timestamp"::TIMESTAMP                            AS event_timestamp,
      data_by_row:"event"::VARCHAR                                  AS event_type,
      data_by_row:"tags"::VARCHAR                                   AS tags,
      data_by_row:"delivery-status"."code"::VARCHAR                 AS delivery_status_code,
      data_by_row:"delivery-status"."description"::VARCHAR          AS delivery_status_description,
      data_by_row:"log-level"::VARCHAR                              AS log_level,
      data_by_row:"url"::VARCHAR                                    AS url,
      data_by_row:"recipient"::VARCHAR                              AS recipient,
      data_by_row:"envelope"."sender"::VARCHAR                      AS sender,
      data_by_row:"envelope"."targets"::VARCHAR                     AS targets,
      data_by_row:"message"."headers"."subject"::VARCHAR            AS subject,
      data_by_row:"geolocation"."city"::VARCHAR                     AS city,
      data_by_row:"geolocation"."region"::VARCHAR                   AS region,
      data_by_row:"geolocation"."country"::VARCHAR                  AS country,
      data_by_row:"flags":"is-routed"::BOOLEAN                      AS is_routed,
      data_by_row:"flags":"is-authenticated"::BOOLEAN               AS is_authenticated,
      data_by_row:"flags":"is-system-test"::BOOLEAN                 AS is_system_test,
      data_by_row:"flags":"is-test-mode"::BOOLEAN                   AS is_test_mode,
      uploaded_at::TIMESTAMP                                        AS uploaded_at

    FROM intermediate
)

SELECT * 
FROM parsed


