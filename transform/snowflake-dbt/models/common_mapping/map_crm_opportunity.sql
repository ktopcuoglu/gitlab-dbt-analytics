WITH crm_account_dimensions AS (

    SELECT *
    FROM {{ ref('map_crm_account') }}

), order_type AS (

    SELECT *
    FROM {{ ref('dim_order_type') }}

), opportunity_source AS (

    SELECT *
    FROM {{ ref('dim_opportunity_source') }}

), purchase_channel AS (

    SELECT *
    FROM {{ ref('dim_purchase_channel') }}

), sales_segment AS (

    SELECT *
    FROM {{ ref('dim_sales_segment') }}

), sfdc_opportunity AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity') }}

), opportunity_fields AS(

    SELECT

      opportunity_id                                            AS dim_crm_opportunity_id,
      account_id                                                AS dim_crm_account_id,
      owner_id                                                  AS dim_crm_sales_rep_id,
      deal_path,
      order_type_stamped                                        AS order_type,
      sales_segment,
      sales_qualified_source

    FROM sfdc_opportunity

), opportunities_with_keys AS (

    SELECT
      opportunity_fields.dim_crm_opportunity_id,
      {{ get_keyed_nulls('opportunity_fields.dim_crm_sales_rep_id') }}                                                 AS dim_crm_sales_rep_id,
      {{ get_keyed_nulls('order_type.dim_order_type_id') }}                                                            AS dim_order_type_id,
      {{ get_keyed_nulls('opportunity_source.dim_opportunity_source_id') }}                                            AS dim_opportunity_source_id,
      {{ get_keyed_nulls('purchase_channel.dim_purchase_channel_id') }}                                                AS dim_purchase_channel_id,
      crm_account_dimensions.dim_parent_crm_account_id,
      crm_account_dimensions.dim_account_crm_account_id,
      crm_account_dimensions.dim_parent_sales_segment_id,
      crm_account_dimensions.dim_parent_geo_region_id,
      crm_account_dimensions.dim_parent_geo_sub_region_id,
      crm_account_dimensions.dim_parent_geo_area_id,
      crm_account_dimensions.dim_parent_sales_territory_id,
      crm_account_dimensions.dim_parent_industry_id,
      crm_account_dimensions.dim_parent_location_country_id,
      crm_account_dimensions.dim_parent_location_region_id,
      {{ get_keyed_nulls('crm_account_dimensions.dim_account_sales_segment_id,sales_segment.dim_sales_segment_id') }}  AS dim_account_sales_segment_id,
      crm_account_dimensions.dim_account_geo_region_id,
      crm_account_dimensions.dim_account_geo_sub_region_id,
      crm_account_dimensions.dim_account_geo_area_id,
      crm_account_dimensions.dim_account_sales_territory_id,
      crm_account_dimensions.dim_account_industry_id,
      crm_account_dimensions.dim_account_location_country_id,
      crm_account_dimensions.dim_account_location_region_id

    FROM opportunity_fields
    LEFT JOIN crm_account_dimensions
      ON opportunity_fields.dim_crm_account_id = crm_account_dimensions.dim_account_crm_account_id
    LEFT JOIN opportunity_source
      ON opportunity_fields.sales_qualified_source = opportunity_source.opportunity_source_name
    LEFT JOIN order_type
      ON opportunity_fields.order_type = order_type.order_type_name
    LEFT JOIN purchase_channel
      ON opportunity_fields.deal_path = purchase_channel.purchase_channel_name
    LEFT JOIN sales_segment
      ON opportunity_fields.sales_segment = sales_segment.sales_segment_name

)

{{ dbt_audit(
    cte_ref="opportunities_with_keys",
    created_by="@snalamaru",
    updated_by="@smcooperDD",
    created_date="2020-12-17",
    updated_date="2021-02-02"
) }}
