WITH invoice AS (

    SELECT *
    FROM {{ ref('zuora_invoice_source') }}

), opp AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_source') }}
    WHERE is_deleted = 'FALSE'

), opp_relational_fields AS (

    SELECT *
    FROM {{ ref('map_crm_opportunity') }}

), quote AS (

    SELECT *
    FROM {{ ref('sfdc_zqu_quote_source') }}
    WHERE is_deleted = 'FALSE'

), quote_amendment AS (

    SELECT *
    FROM {{ ref('sfdc_zqu_quote_amendment_source') }}
    WHERE is_deleted = 'FALSE'

), rate_plan AS (

    SELECT *
    FROM {{ ref('sfdc_zqu_quote_rate_plan_source') }}
    WHERE is_deleted = 'FALSE'

), rate_plan_charge AS (

    SELECT *
    FROM {{ ref('sfdc_zqu_quote_rate_plan_charge_source') }}
    WHERE is_deleted = 'FALSE'

), quote_items AS (

    SELECT

      --ids
      {{ dbt_utils.surrogate_key(['CONCAT(quote_amendment.zqu_quote_amendment_id,
                                   COALESCE(rate_plan_charge.zqu_quote_rate_plan_charge_id, MD5(-1)),  -- this should be the get_keyed_nulls macro
                                   COALESCE(rate_plan_charge.zqu_product_rate_plan_charge_zuora_id, MD5(-1)))']
                                ) }}                                                                AS quote_item_id,
      quote_amendment.zqu_quote_amendment_id                                                        AS quote_amendment_id,
      quote.quote_id                                                                                AS dim_quote_id,
      quote.owner_id                                                                                AS dim_crm_sales_rep_id,

      --relational keys
      quote.zqu__account                                                                            AS dim_crm_account_id,
      quote.zqu__zuora_account_id                                                                   AS dim_billing_account_id,
      quote.zqu__zuora_subscription_id                                                              AS dim_subscription_id,
      opp.opportunity_id                                                                            AS dim_crm_opportunity_id,
      opp_relational_fields.dim_crm_sales_rep_id                                                    AS opp_dim_crm_sales_rep_id,
      opp_relational_fields.dim_order_type_id                                                       AS opp_dim_order_type_id,
      opp_relational_fields.dim_opportunity_source_id                                               AS opp_dim_opportunity_source_id,
      opp_relational_fields.dim_purchase_channel_id                                                 AS opp_dim_purchase_channel_id,
      opp_relational_fields.dim_sales_segment_id                                                    AS opp_dim_sales_segment_id,
      opp_relational_fields.dim_sales_territory_id                                                  AS opp_dim_sales_territory_id,
      opp_relational_fields.dim_industry_id                                                         AS opp_dim_industry_id,
      invoice.invoice_id                                                                            AS dim_invoice_id,
      rate_plan.zqu_subscription_rate_plan_zuora_id                                                 AS rate_plan_id,
      rate_plan.zqu_product_rate_plan_zuora_id                                                      AS product_rate_plan_id,
      rate_plan_charge.zqu_subscription_rate_plan_charge_zuora_id                                   AS rate_plan_charge_id,
      rate_plan_charge.zqu_quote_rate_plan_charge_id                                                AS quote_rate_plan_charge_id,
      rate_plan_charge.zqu_product_rate_plan_charge_zuora_id                                        AS dim_product_detail_id,

      --additive fields
      quote_amendment.zqu__total_amount                                                             AS total_amount,
      quote_amendment.license_amount                                                                AS license_amount,
      quote_amendment.professional_services_amount                                                  AS professional_services_amount,
      quote_amendment.true_up_amount                                                                AS true_up_amount,
      quote_amendment.zqu__delta_mrr                                                                AS delta_mrr,
      quote_amendment.zqu__delta_tcv                                                                AS delta_tcv,
      rate_plan_charge.zqu_mrr                                                                      AS mrr,
      rate_plan_charge.zqu_mrr * 12                                                                 AS arr,
      rate_plan_charge.zqu_tcv                                                                      AS tcv

    FROM quote_amendment
    INNER JOIN quote
      ON quote_amendment.zqu__quote = quote.zqu_quote_id
    INNER JOIN opp
      ON quote.zqu__opportunity = opp.opportunity_id
    INNER JOIN opp_relational_fields
      ON opp.opportunity_id = opp_relational_fields.dim_crm_opportunity_id
    LEFT JOIN invoice
      ON opp.invoice_number = invoice.invoice_number
    INNER JOIN rate_plan
      ON  quote_amendment.zqu_quote_amendment_id = rate_plan.zqu_quote_amendment_id
    INNER JOIN rate_plan_charge
      ON rate_plan.zqu_quote_rate_plan_id = rate_plan_charge.zqu_quote_rate_plan_id

)

{{ dbt_audit(
    cte_ref="quote_items",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2021-01-12",
    updated_date="2021-01-12"
) }}
