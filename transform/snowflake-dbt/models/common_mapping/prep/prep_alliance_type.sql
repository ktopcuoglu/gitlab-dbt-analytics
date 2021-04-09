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
      {{ alliance_type('partner_account.account_name', 'influence_partner.account_name',
                       'sfdc_opportunity_source.partner_account', 'sfdc_opportunity_source.influence_partner') }},
      {{ alliance_type_short('partner_account.account_name', 'influence_partner.account_name',
                             'sfdc_opportunity_source.partner_account', 'sfdc_opportunity_source.influence_partner') }}
    FROM sfdc_opportunity_source
    LEFT JOIN sfdc_account_source      AS partner_account
      ON sfdc_opportunity_source.partner_account = partner_account.account_id
    LEFT JOIN sfdc_account_source      AS influence_partner
      ON sfdc_opportunity_source.influence_partner = influence_partner.account_id

), final AS (

    SELECT DISTINCT
      {{ dbt_utils.surrogate_key(['alliance_type']) }}  AS dim_alliance_type_id,
      alliance_type                                     AS alliance_type_name,
      alliance_type_short                               AS alliance_type_short_name
    FROM alliance_type

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2021-04-07",
    updated_date="2021-04-07"
) }}
