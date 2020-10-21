WITH sfdc_opportunity AS (

  SELECT *
  FROM {{ ref('sfdc_opportunity')}}

), first_contact  AS (

  SELECT
    opportunity_id,                                                           -- opportunity_id
		contact_id                                                                AS sfdc_contact_id,
    {{ dbt_utils.surrogate_key(['contact_id']) }}                             AS crm_person_id,
    ROW_NUMBER() OVER (PARTITION BY opportunity_id ORDER BY created_date ASC) AS row_num
  FROM {{ ref('sfdc_opportunity_contact_role')}}

), opporunity_fields AS( 

  SELECT
    opportunity_id,      -- opportunity_id
    account_id           AS crm_account_id,
    owner_id             AS crm_sales_rep_id,
		incremental_acv      AS iacv,
    created_date,        -- created_date
    sales_accepted_date, -- sales_accepted_date
    close_date,          -- close_date
    is_closed,           -- is_closed
    is_won               -- is_won
  FROM sfdc_opportunity

), is_sao AS (

  SELECT
    opportunity_id,
    CASE
      WHEN sfdc_opportunity.sales_accepted_date IS NOT NULL
        AND is_edu_oss = 0
        AND stage_name != '10-Duplicate'
        AND order_type_stamped = '1. New - First Order'
          THEN TRUE
    	ELSE FALSE
    END AS is_sao
  FROM sfdc_opportunity

), is_sdr_sao AS (

  SELECT
    opportunity_id,
    CASE
      WHEN opportunity_id in (select opportunity_id from is_sao)
        AND sales_qualified_source IN (
                                      'SDR Generated'
                                      , 'BDR Generated'
                                      )
          THEN TRUE
      ELSE FALSE
    END AS is_sdr_sao
  FROM sfdc_opportunity

), joined AS (

SELECT
  opporunity_fields.opportunity_id,
  opporunity_fields.crm_account_id,
  opporunity_fields.crm_sales_rep_id,
  first_contact.crm_person_id,
  first_contact.sfdc_contact_id,
  opporunity_fields.created_date,
  opporunity_fields.sales_accepted_date,
  opporunity_fields.close_date,
  opporunity_fields.is_closed,
  opporunity_fields.is_won,
  is_sao.is_sao,
  is_sdr_sao.is_sdr_sao,
  opporunity_fields.iacv
FROM opporunity_fields
LEFT JOIN first_contact
  ON opporunity_fields.opportunity_id = first_contact.opportunity_id AND row_num = 1
LEFT JOIN is_sao
  ON opporunity_fields.opportunity_id = is_sao.opportunity_id
LEFT JOIN is_sdr_sao
  ON opporunity_fields.opportunity_id = is_sdr_sao.opportunity_id

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@jjstark ",
    updated_by="@jjstark",
    created_date="2020-09-15",
    updated_date="2020-10-21"
) }}
