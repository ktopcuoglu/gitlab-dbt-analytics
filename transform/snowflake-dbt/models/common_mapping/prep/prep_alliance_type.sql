WITH sfdc_opportunity_source AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_source') }}
    WHERE NOT is_deleted

), sfdc_account_source AS (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}
    WHERE NOT is_deleted

), alliance_type AS (

    SELECT
      CASE
        WHEN LOWER(partner_account.account_name) LIKE '%google%' OR LOWER(influence_partner.account_name) LIKE '%google%'
          THEN 'GCP'
        WHEN LOWER(partner_account.account_name) LIKE ANY ('%aws%', '%amazon%') OR LOWER(influence_partner.account_name) LIKE ANY ('%aws%', '%amazon%')
          THEN 'AWS'
        WHEN LOWER(partner_account.account_name) LIKE '%ibm (oem)%' OR LOWER(influence_partner.account_name) LIKE '%ibm (oem)%'
          THEN 'IBM'
        ELSE 'Non-Alliance Partners'
      END                              AS alliance_type_short,
      CASE
        WHEN LOWER(partner_account.account_name) LIKE '%google%' OR LOWER(influence_partner.account_name) LIKE '%google%'
          THEN 'Google Cloud'
        WHEN LOWER(partner_account.account_name) LIKE ANY ('%aws%', '%amazon%') OR LOWER(influence_partner.account_name) LIKE ANY ('%aws%', '%amazon%')
          THEN 'Amazon Web Services'
        WHEN LOWER(partner_account.account_name) LIKE '%ibm (oem)%' OR LOWER(influence_partner.account_name) LIKE '%ibm (oem)%'
          THEN 'IBM (OEM)'
        ELSE 'Non-Alliance Partners'
      END                              AS alliance_type
    FROM sfdc_opportunity_source
    LEFT JOIN sfdc_account_source      AS partner_account
      ON sfdc_opportunity_source.partner_account = partner_account.account_id
    LEFT JOIN sfdc_account_source      AS influence_partner
      ON sfdc_opportunity_source.influence_partner = influence_partner.account_id

), unioned AS (

    SELECT DISTINCT
      {{ dbt_utils.surrogate_key(['alliance_type']) }}  AS dim_alliance_type_id,
      alliance_type                                     AS alliance_type_name,
      alliance_type_short                               AS alliance_type_short_name
    FROM alliance_type

    UNION ALL

    SELECT
      MD5('-1')                                          AS dim_alliance_type_id,
      'Missing alliance_type_name'                       AS alliance_type_name,
      'Missing alliance_type_short_name'                 AS alliance_type_short_name

)

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2021-04-07",
    updated_date="2021-04-07"
) }}
