WITH crm_account AS (

    SELECT *
    FROM {{ ref('map_crm_account') }}

), invoice AS (

    SELECT *
    FROM {{ ref('zuora_invoice_source') }}
    WHERE is_deleted = 'FALSE'

), opportunity_dimensions AS (

    SELECT *
    FROM {{ ref('map_crm_opportunity') }}

), quote AS (

    SELECT *
    FROM {{ ref('sfdc_zqu_quote_source') }}
    WHERE is_deleted = 'FALSE'

), final_quotes AS (

    SELECT

      --ids
      quote.zqu_quote_id                                AS dim_quote_id,
      quote.zqu__account                                AS dim_crm_account_id,
      crm_account.dim_parent_crm_account_id,
      quote.zqu__zuora_account_id                       AS dim_billing_account_id,

      --shared dimension keys
      quote.zqu__opportunity                            AS dim_crm_opportunity_id,
      quote.zqu__zuora_subscription_id                  AS dim_subscription_id,
      quote.owner_id                                    AS dim_crm_sales_rep_id,
      opportunity_dimensions.dim_crm_sales_rep_id       AS opp_dim_crm_sales_rep_id,
      opportunity_dimensions.dim_order_type_id          AS opp_dim_order_type_id,
      opportunity_dimensions.dim_opportunity_source_id  AS opp_dim_opportunity_source_id,
      opportunity_dimensions.dim_purchase_channel_id    AS opp_dim_purchase_channel_id,
      crm_account.dim_parent_sales_segment_id,
      crm_account.dim_parent_geo_region_id,
      crm_account.dim_parent_geo_sub_region_id,
      crm_account.dim_parent_geo_area_id,
      crm_account.dim_parent_sales_territory_id,
      crm_account.dim_parent_industry_id,
      crm_account.dim_parent_location_country_id,
      crm_account.dim_parent_location_region_id,
      crm_account.dim_account_sales_segment_id,
      crm_account.dim_account_geo_region_id,
      crm_account.dim_account_geo_sub_region_id,
      crm_account.dim_account_geo_area_id,
      crm_account.dim_account_sales_territory_id,
      crm_account.dim_account_industry_id,
      crm_account.dim_account_location_country_id,
      crm_account.dim_account_location_region_id,
      invoice.invoice_id                                AS dim_invoice_id,

      --dates
      quote.created_date,
      quote.quote_end_date,
      quote.zqu__valid_until                            AS quote_valid_until

    FROM quote
    LEFT JOIN opportunity_dimensions
      ON quote.zqu__opportunity = opportunity_dimensions.dim_crm_opportunity_id
    LEFT JOIN invoice
      ON quote.invoice_number = invoice.invoice_number
    LEFT JOIN crm_account
      ON quote.zqu__account = crm_account.dim_account_crm_account_id

)

{{ dbt_audit(
cte_ref="final_quotes",
created_by="@mcooperDD",
updated_by="@mcooperDD",
created_date="2021-01-11",
updated_date="2021-02-02"
) }}
