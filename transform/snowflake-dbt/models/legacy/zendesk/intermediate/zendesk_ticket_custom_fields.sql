{{config({
    "schema": "legacy"
  })
}}

WITH zendesk_custom_fields AS (

    SELECT *
    FROM {{ref('zendesk_ticket_fields_source')}}

), filtered AS (

    SELECT
      ID                                     AS ticket_field_id,
      title                                  AS ticket_form_title,
      value,
      REPLACE(value:"value", '"', '')        AS ticket_field_value,
      REPLACE(VALUE:"name", '"', '')         AS ticket_field_name
    FROM
      zendesk_ticket_fields,
    lateral FLATTEN(input => PARSE_JSON(custom_field_options))
)

SELECT *
FROM filtered
