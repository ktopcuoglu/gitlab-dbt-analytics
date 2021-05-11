WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sdr_attainment_to_goal') }}

), renamed as (

    SELECT
      current_month::DATE                   AS current_month,
      name::VARCHAR                         AS name,
      sdr_sfdc_name::VARCHAR                AS sdr_sfdc_name,
      role::VARCHAR                         AS role,
      status::VARCHAR                       AS status,
      region::VARCHAR                       AS region,
      segment::VARCHAR                      AS segment,
      type::VARCHAR                         AS type,
      total_leads_accepted::NUMBER          AS total_leads_accepted,
      accepted_leads::NUMBER                AS accepted_leads,
      accepted_leads_qualifying::NUMBER     AS accepted_leads_qualifying,
      accepted_leads_completed::NUMBER      AS accepted_leads_completed,
      leads_worked::NUMBER                  AS leads_worked,
      qualified_leads::NUMBER               AS qualified_leads,
      unqualified_leads::NUMBER             AS unqualified_leads,
      accepted_leads_inbound::NUMBER        AS accepted_leads_inbound,
      accepted_leads_outbound::NUMBER       AS accepted_leads_outbound,
      inbound_leads_worked::NUMBER          AS inbound_leads_worked,
      outbound_leads_worked::NUMBER         AS outbound_leads_worked,
      inbound_leads_accepted::NUMBER        AS inbound_leads_accepted,
      outbound_leads_accepted::NUMBER       AS outbound_leads_accepted,
      inbound_leads_qualifying::NUMBER      AS inbound_leads_qualifying,
      outbound_leads_qualifying::NUMBER     AS outbound_leads_qualifying,
      iqm::NUMBER                           AS iqm,
      average_working_day_calls::NUMBER     AS average_working_day_calls,
      average_working_day_emails::NUMBER    AS average_working_day_emails,
      average_working_day_other::NUMBER     AS average_working_day_other,
      saos::NUMBER                          AS saos,
      quarterly_sao_target::NUMBER          AS quarterly_sao_target,
      quarterly_sao_variance::NUMBER        AS quarterly_sao_variance,
      average_time::NUMBER                  AS average_time,
      end_of_month::DATE                    AS end_of_month
    FROM source
)

SELECT *
FROM renamed
