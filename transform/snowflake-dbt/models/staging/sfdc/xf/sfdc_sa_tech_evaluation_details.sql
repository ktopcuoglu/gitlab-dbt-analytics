WITH sfdc_opportunity AS (

    SELECT * FROM {{ref('sfdc_opportunity_source')}}

), final AS (

    SELECT
      -- keys
      sfdc_opportunity.account_id,
      sfdc_opportunity.opportunity_id,
      sfdc_opportunity.owner_id,
      sfdc_opportunity.sa_tech_evaluation_close_status,
      sfdc_opportunity.sa_tech_evaluation_end_date,
      sfdc_opportunity.sa_tech_evaluation_start_date
    FROM sfdc_opportunity

)


{{ dbt_audit(
    cte_ref="final",
    created_by="@paul_armstrong",
    updated_by="@paul_armstrong",
    created_date="2020-11-18",
    updated_date="2020-11-18"
) }}
