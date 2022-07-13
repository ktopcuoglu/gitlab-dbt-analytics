WITH source                      AS (

  SELECT *
  FROM {{ source('google_ads', 'ad_group_hourly_stats') }}

), final                      AS (
    
    SELECT 
      id::NUMBER                                    AS hourly_stats_id,
      customer_id::NUMBER                           AS customer_id,
      date::DATE                                    AS date,
      campaign_base_campaign::TEXT                  AS campaign_base_campaign,
      click_type::TEXT                              AS click_type,
      conversions::FLOAT                            AS conversions,
      month::DATE                                   AS month,
      interactions::NUMBER                          AS interactions,
      average_cpm::FLOAT                            AS average_cpm,
      year::NUMBER                                  AS year,
      interaction_event_types::TEXT                 AS interaction_event_types,
      campaign_id::NUMBER                           AS campaign_id,
      device::TEXT                                  AS device,
      week::DATE                                    AS week,
      active_view_impressions::NUMBER               AS active_view_impressions,
      clicks::NUMBER                                AS clicks,
      active_view_measurable_impressions::NUMBER    AS active_view_measurable_impressions,
      day_of_week::TEXT                             AS day_of_week,
      quarter::DATE                                 AS quarter,
      cost_per_conversion::FLOAT                    AS cost_per_conversion,
      active_view_measurability::FLOAT              AS active_view_measurability,
      average_cpc::FLOAT                            AS average_cpc,
      ctr::FLOAT                                    AS ctr,
      conversions_value::FLOAT                      AS conversions_value,
      average_cost::FLOAT                           AS average_cost,
      ad_network_type::TEXT                         AS ad_network_type,
      interaction_rate::FLOAT                       AS interaction_rate,
      impressions::NUMBER                           AS impressions,
      active_view_viewability::FLOAT                AS active_view_viewability,
      value_per_conversion::FLOAT                   AS value_per_conversion,
      hour::NUMBER                                  AS hour,
      active_view_cpm::FLOAT                        AS active_view_cpm,
      active_view_ctr::FLOAT                        AS active_view_ctr,
      active_view_measurable_cost_micros::NUMBER    AS active_view_measurable_cost_micros,
      base_ad_group::TEXT                           AS base_ad_group,
      conversions_from_interactions_rate::FLOAT     AS conversions_from_interactions_rate,
      cost_micros::NUMBER                           AS cost_micros,
      _fivetran_synced::TIMESTAMP_TZ                AS fivetran_synced
    FROM source 
)

SELECT *
FROM final
