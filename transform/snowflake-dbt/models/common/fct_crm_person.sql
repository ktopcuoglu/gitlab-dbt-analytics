WITH account_dims_mapping AS (

  SELECT *
  FROM {{ ref('map_crm_account') }}

), crm_person AS (

  SELECT

    dim_crm_person_id,
    sfdc_record_id,
    bizible_person_id,
    bizible_touchpoint_position,
    bizible_marketing_channel_path,
    bizible_touchpoint_date,
    dim_crm_account_id,
    dim_crm_sales_rep_id,
    person_score

    FROM {{ref('prep_crm_person')}}

), geo_region AS (

    SELECT *
    FROM {{ ref('prep_geo_region') }}

), geo_sub_region AS (

    SELECT *
    FROM {{ ref('prep_geo_sub_region') }}

), industry AS (

    SELECT *
    FROM {{ ref('prep_industry') }}

), marketing_channel AS (

    SELECT *
    FROM {{ ref('prep_marketing_channel') }}

), marketing_channel_mapping AS (

    SELECT *
    FROM {{ ref('map_marketing_channel') }}

), sales_segment AS (

      SELECT *
      FROM {{ ref('dim_sales_segment') }}

), sales_territory AS (

    SELECT *
    FROM {{ ref('prep_sales_territory') }}

), sfdc_contacts AS (

    SELECT *
    FROM {{ ref('sfdc_contact_source') }}
    WHERE is_deleted = 'FALSE'

), sfdc_leads AS (

    SELECT *
    FROM {{ ref('sfdc_lead_source') }}
    WHERE is_deleted = 'FALSE'

) , marketing_qualified_leads AS(

    SELECT

      {{ dbt_utils.surrogate_key(['COALESCE(converted_contact_id, lead_id)','marketo_qualified_lead_date::timestamp']) }} AS event_id,
      marketo_qualified_lead_date::timestamp                                                                              AS event_timestamp,
      lead_id                                                                                                             AS sfdc_record_id,
      'lead'                                                                                                              AS sfdc_record,
      {{ dbt_utils.surrogate_key(['COALESCE(converted_contact_id, lead_id)']) }}                                          AS crm_person_id,
      converted_contact_id                                                                                                AS contact_id,
      converted_account_id                                                                                                AS account_id,
      owner_id                                                                                                            AS crm_sales_rep_id,
      person_score                                                                                                        AS person_score

    FROM sfdc_leads
    WHERE marketo_qualified_lead_date IS NOT NULL

), marketing_qualified_contacts AS(

    SELECT

      {{ dbt_utils.surrogate_key(['contact_id','marketo_qualified_lead_date::timestamp']) }}                              AS event_id,
      marketo_qualified_lead_date::timestamp                                                                              AS event_timestamp,
      contact_id                                                                                                          AS sfdc_record_id,
      'contact'                                                                                                           AS sfdc_record,
      {{ dbt_utils.surrogate_key(['contact_id']) }}                                                                       AS crm_person_id,
      contact_id                                                                                                          AS contact_id,
      account_id                                                                                                          AS account_id,
      owner_id                                                                                                            AS crm_sales_rep_id,
      person_score                                                                                                        AS person_score

    FROM sfdc_contacts
    WHERE marketo_qualified_lead_date IS NOT NULL
    HAVING event_id NOT IN (
                         SELECT event_id
                         FROM marketing_qualified_leads
                         )

), mqls_unioned AS (

    SELECT *
    FROM marketing_qualified_leads

    UNION

    SELECT *
    FROM marketing_qualified_contacts

), mqls AS (

    SELECT

      crm_person_id,
      MIN(event_timestamp)  AS first_mql_date,
      MAX(event_timestamp)  AS last_mql_date,
      COUNT(*)              AS mql_count

    FROM mqls_unioned
    GROUP BY 1

), final AS (

    SELECT
    -- ids
      crm_person.dim_crm_person_id                                                                                        AS dim_crm_person_id,
      crm_person.sfdc_record_id                                                                                           AS sfdc_record_id,
      crm_person.bizible_person_id                                                                                        AS bizible_person_id,

     -- common dimension keys
      crm_person.dim_crm_sales_rep_id                                                                                     AS dim_crm_sales_rep_id,
      crm_person.dim_crm_account_id                                                                                       AS dim_crm_account_id,
      account_dims_mapping.parent_dim_crm_account_id,
      COALESCE(account_dims_mapping.account_dim_sales_segment_id, sales_segment.dim_sales_segment_id)                     AS account_dim_sales_segment_id,
      COALESCE(account_dims_mapping.account_dim_geo_region_id, geo_region.dim_geo_region_id)                              AS account_dim_geo_region_id,
      COALESCE(account_dims_mapping.account_dim_geo_sub_region_id, geo_sub_region.dim_geo_sub_region_id)                  AS account_dim_geo_sub_region_id,
      account_dims_mapping.account_dim_geo_area_id                                                                        AS account_dim_geo_area_id,
      COALESCE(account_dims_mapping.account_dim_sales_territory_id, sales_territory.dim_sales_territory_id)               AS account_dim_sales_territory_id,
      COALESCE(account_dims_mapping.account_dim_industry_id, industry.dim_industry_id)                                    AS account_dim_industry_id,
      account_dims_mapping.account_dim_location_country_id,
      account_dims_mapping.account_dim_location_region_id,
      account_dims_mapping.parent_dim_sales_segment_id,
      account_dims_mapping.parent_dim_geo_region_id,
      account_dims_mapping.parent_dim_geo_sub_region_id,
      account_dims_mapping.parent_dim_geo_area_id,
      account_dims_mapping.parent_dim_sales_territory_id,
      account_dims_mapping.parent_dim_industry_id,
      account_dims_mapping.parent_dim_location_country_id,
      account_dims_mapping.parent_dim_location_region_id,
      {{ get_keyed_nulls('marketing_channel.dim_marketing_channel_id') }}                                                 AS dim_marketing_channel_id,

     -- important person dates
      COALESCE(sfdc_contacts.created_date, sfdc_leads.created_date)::DATE                                                 AS created_date,
      {{ get_date_id('COALESCE(sfdc_contacts.created_date, sfdc_leads.created_date)') }}                                  AS created_date_id,
      COALESCE(sfdc_contacts.inquiry_datetime, sfdc_leads.inquiry_datetime)::DATE                                         AS inquiry_date,
      {{ get_date_id('inquiry_date') }}                                                                                   AS inquiry_date_id,
      mqls.first_mql_date::DATE                                                                                           AS mql_date_first,
      {{ get_date_id('mql_date_first') }}                                                                                 AS mql_date_first_id,
      mqls.last_mql_date::DATE                                                                                            AS mql_date_latest,
      {{ get_date_id('last_mql_date') }}                                                                                  AS mql_date_latest_id,
      COALESCE(sfdc_contacts.accepted_datetime, sfdc_leads.accepted_datetime)::DATE                                       AS accepted_date,
      {{ get_date_id('accepted_date') }}                                                                                  AS accepted_date_id,
      COALESCE(sfdc_contacts.qualifying_datetime, sfdc_leads.qualifying_datetime)::DATE                                   AS qualifying_date,
      {{ get_date_id('qualifying_date') }}                                                                                AS qualifying_date_id,
      COALESCE(sfdc_contacts.qualified_datetime, sfdc_leads.qualified_datetime)::DATE                                     AS qualified_date,
      {{ get_date_id('qualified_date') }}                                                                                 AS qualified_date_id,
      sfdc_leads.converted_date::DATE                                                                                     AS converted_date,
      {{ get_date_id('converted_date') }}                                                                                 AS converted_date_id,

     -- flags
      CASE
          WHEN mqls.first_mql_date IS NOT NULL THEN 1
          ELSE 0
        END                                                                                                               AS is_mql,
      CASE
        WHEN COALESCE(LOWER(sfdc_contacts.contact_status), LOWER(sfdc_leads.lead_status)) = 'inquiry' THEN 1
        ELSE 0
      END                                                                                                                 AS is_inquiry,

     -- additive fields

      crm_person.person_score                                                                                             AS person_score,
      mqls.mql_count                                                                                                      AS mql_count

    FROM crm_person
    LEFT JOIN sfdc_leads
      ON crm_person.sfdc_record_id = sfdc_leads.lead_id
    LEFT JOIN sfdc_contacts
      ON crm_person.sfdc_record_id = sfdc_contacts.contact_id
    LEFT JOIN mqls
      ON crm_person.dim_crm_person_id = mqls.crm_person_id
    LEFT JOIN account_dims_mapping
      ON crm_person.dim_crm_account_id = account_dims_mapping.account_dim_crm_account_id
    LEFT JOIN sales_segment
      ON sfdc_leads.sales_segmentation = sales_segment.sales_segment_name
    LEFT JOIN geo_region
      ON sfdc_leads.tsp_region = geo_region.geo_region_name
    LEFT JOIN geo_sub_region
      ON sfdc_leads.tsp_sub_region = geo_sub_region.geo_sub_region_name
    LEFT JOIN sales_territory
      ON sfdc_leads.tsp_territory = sales_territory.sales_territory_name
    LEFT JOIN industry
      ON COALESCE(sfdc_contacts.industry, sfdc_leads.industry) = industry.industry_name
    LEFT JOIN marketing_channel_mapping
      ON crm_person.bizible_marketing_channel_path = marketing_channel_mapping.bizible_marketing_channel_path
    LEFT JOIN marketing_channel
      ON marketing_channel_mapping.marketing_channel_name = marketing_channel.marketing_channel_name

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2020-12-01",
    updated_date="2021-01-28"
) }}
