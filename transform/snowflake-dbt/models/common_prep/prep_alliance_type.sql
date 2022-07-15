{{ config(
    tags=["mnpi_exception"]
) }}

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
      {{ alliance_type('fulfillment_partner.account_name', 'partner_account.account_name',
                       'sfdc_opportunity_source.close_date', 'sfdc_opportunity_source.partner_track',
                       'sfdc_opportunity_source.resale_partner_track', 'sfdc_opportunity_source.deal_path') }},
      {{ alliance_type_short('fulfillment_partner.account_name', 'partner_account.account_name',
                             'sfdc_opportunity_source.close_date', 'sfdc_opportunity_source.partner_track',
                             'sfdc_opportunity_source.resale_partner_track', 'sfdc_opportunity_source.deal_path') }}
    FROM sfdc_opportunity_source
    LEFT JOIN sfdc_account_source      AS fulfillment_partner
      ON sfdc_opportunity_source.fulfillment_partner = fulfillment_partner.account_id
    LEFT JOIN sfdc_account_source AS partner_account
      ON sfdc_opportunity_source.partner_account = partner_account.account_id
    WHERE alliance_type IS NOT NULL

), final AS (

    SELECT DISTINCT
      {{ dbt_utils.surrogate_key(['alliance_type']) }}  AS dim_alliance_type_id,
      alliance_type                                     AS alliance_type_name,
      alliance_type_short                               AS alliance_type_short_name
    FROM alliance_type

    UNION ALL

    SELECT
      MD5('-1')                                         AS dim_alliance_type_id,
      'Missing alliance_type_name'                      AS alliance_type_name,
      'Missing alliance_type_short_name'                AS alliance_type_short_name

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@jpeguero",
    created_date="2021-04-07",
    updated_date="2021-09-15"
) }}
