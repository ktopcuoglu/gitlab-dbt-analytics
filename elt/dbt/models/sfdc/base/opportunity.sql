WITH stages AS (
        SELECT * FROM {{ ref('mapped_stages') }}
)

SELECT
  o.id             AS sfdc_id,
  accountid,
  stagename,
  leadsource,
  TYPE,
  createddate AS created_date,
  closedate,
  sql_source__c,
  competitors__c,
  sales_segmentation_o__c,
  sales_qualified_date__c,
  sales_accepted_date__c,
  name,
  ownerid,
  weighted_iacv__c,
  current_date - greatest(
      x0_pending_acceptance_date__c,
      x1_discovery_date__c,
      x2_scoping_date__c,
      x3_technical_evaluation_date__c,
      x4_proposal_date__c,
      x5_negotiating_date__c,
      x6_closed_won_date__c,
      x7_closed_lost_date__c,
      x8_unqualified_date__c
  ) :: DATE + 1  AS days_in_stage,
  CASE
  WHEN incremental_acv__c > 100000
    THEN TRUE
  ELSE FALSE END AS over_100k,
  CASE WHEN
    incremental_acv_2__c :: DECIMAL < 5000
    THEN '1 - Small (<5k)'
  WHEN incremental_acv_2__c :: DECIMAL >= 5000 AND incremental_acv_2__c :: DECIMAL < 25000
    THEN '2 - Medium (5k - 25k)'
  WHEN incremental_acv_2__c :: DECIMAL >= 25000 AND incremental_acv_2__c :: DECIMAL < 100000
    THEN '3 - Big (25k - 100k)'
  WHEN incremental_acv_2__c :: DECIMAL >= 100000
    THEN '4 - Jumbo (>100k)'
  ELSE '5 - Unknown' END                                          AS deal_size,
  push_counter__c,
  s.is_won,
  lastactivitydate -- will need to be replaced
FROM sfdc.opportunity o
INNER JOIN stages s ON o.stagename=s.masterlabel
WHERE o.isdeleted = FALSE