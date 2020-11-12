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

), opportunity_fields AS( 

  SELECT
    opportunity_id                           AS crm_opportunity_id,
    account_id                               AS crm_account_id,
    owner_id                                 AS crm_sales_rep_id,
    incremental_acv                          AS iacv,
    created_date,                            -- created_date
    {{ get_date_id('created_date') }}        AS created_date_id,
    sales_accepted_date,                     -- sales_accepted_date
    {{ get_date_id('sales_accepted_date') }} AS sales_accepted_date_id,
    close_date,                              -- close_date
    {{ get_date_id('close_date') }}          AS close_date_id,
    is_closed,                               -- is_closed
    is_won                                   -- is_won
  FROM sfdc_opportunity

), is_sao AS (

  SELECT
    opportunity_id,
    CASE
      WHEN sfdc_opportunity.sales_accepted_date IS NOT NULL
        AND is_edu_oss = 0
        AND stage_name != '10-Duplicate'
          THEN TRUE
    	ELSE FALSE
    END AS is_sao
  FROM sfdc_opportunity

), is_sdr_sao AS (

  SELECT
    opportunity_id,
    CASE
      WHEN opportunity_id in (select opportunity_id from is_sao where is_sao = true)
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
    opportunity_fields.crm_opportunity_id,
    opportunity_fields.crm_account_id,
    opportunity_fields.crm_sales_rep_id,
    first_contact.crm_person_id,
    first_contact.sfdc_contact_id,
    opportunity_fields.created_date,
    opportunity_fields.created_date_id,
    opportunity_fields.sales_accepted_date,
    opportunity_fields.sales_accepted_date_id,
    opportunity_fields.close_date,
    opportunity_fields.close_date_id,
    opportunity_fields.is_closed,
    opportunity_fields.is_won,
    is_sao.is_sao,
    is_sdr_sao.is_sdr_sao,
    opportunity_fields.iacv
  FROM opportunity_fields
  LEFT JOIN first_contact
    ON opportunity_fields.crm_opportunity_id = first_contact.opportunity_id AND row_num = 1
  LEFT JOIN is_sao
    ON opportunity_fields.crm_opportunity_id = is_sao.opportunity_id
  LEFT JOIN is_sdr_sao
    ON opportunity_fields.crm_opportunity_id = is_sdr_sao.opportunity_id

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@jjstark ",
    updated_by="@jjstark",
    created_date="2020-09-15",
    updated_date="2020-10-21"
) }}
