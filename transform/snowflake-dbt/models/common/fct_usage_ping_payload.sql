{%- set settings_columns = dbt_utils.get_column_values(table=ref('prep_usage_ping_metrics_setting'), column='metrics_path', max_records=1000, default=['']) %}

{{ simple_cte([
    ('raw_usage_data', 'version_raw_usage_data_source'),
    ('map_ip_to_country', 'map_ip_to_country'),
    ('locations', 'prep_location_country'),
    ('prep_license', 'prep_license'),
    ('prep_subscription', 'prep_subscription'),
    ('raw_usage_data', 'version_raw_usage_data_source'),
    ('prep_usage_data_flattened', 'prep_usage_data_flattened'),
    ('map_ip_to_country', 'map_ip_to_country'),
    ('locations', 'prep_location_country'),
    ('prep_usage_ping_metrics_setting', 'prep_usage_ping_metrics_setting'),
    ('dim_date', 'dim_date'),
    ])
    
}}

, usage_ping AS (

    SELECT 
      id                                                                        AS dim_usage_ping_id, 
      created_at::TIMESTAMP(0)                                                  AS ping_created_at,
      *, 
      {{ nohash_sensitive_columns('version_usage_data_source', 'source_ip') }}  AS ip_address_hash, 
      OBJECT_CONSTRUCT(
        {% for column in columns %}  
          '{{ column.column | lower }}', {{ column.column | lower }}
          {% if not loop.last %}
            ,
          {% endif %}
        {% endfor %}
      )                                                                         AS raw_usage_data_payload_reconstructed
    FROM {{ ref('version_usage_data_source') }}

-- ), settings_data AS (

--     SELECT
--       ping_id AS dim_usage_ping_id,
--       {% for column in settings_columns %}
--         MAX(IFF(prep_usage_data_flattened.metrics_path = '{{column}}', metric_value, NULL)) AS {{column | replace(".","_")}}
--         {{ "," if not loop.last }}
--       {% endfor %}    
--     FROM prep_usage_data_flattened
--     INNER JOIN prep_usage_ping_metrics_setting ON prep_usage_data_flattened.metrics_path = prep_usage_ping_metrics_setting.metrics_path
--     GROUP BY 1
  
), usage_data AS (

    SELECT
      dim_usage_ping_id, 
      ping_created_at,
      source_ip_hash                                                                                  AS ip_address_hash,
      {{ dbt_utils.star(from=ref('version_usage_data_source'), except=['EDITION', 'CREATED_AT', 'SOURCE_IP']) }},
      IFF(license_expires_at >= ping_created_at OR license_expires_at IS NULL, edition, 'EE Free')    AS cleaned_edition,
      REGEXP_REPLACE(NULLIF(version, ''), '[^0-9.]+')                                                 AS cleaned_version,
        IFF(
            version LIKE '%-pre%' OR version LIKE '%-rc%', 
            TRUE, FALSE
        )::BOOLEAN                                                   AS version_is_prerelease,
      SPLIT_PART(cleaned_version, '.', 1)::NUMBER                                                     AS major_version,
      SPLIT_PART(cleaned_version, '.', 2)::NUMBER                                                     AS minor_version,
      major_version || '.' || minor_version                                                           AS major_minor_version,
      raw_usage_data_payload_reconstructed
    FROM usage_ping
    WHERE uuid IS NOT NULL
      AND version NOT LIKE ('%VERSION%') -- Messy data that's not worth parsing

), joined AS (

    SELECT 
      dim_usage_ping_id,
      ping_created_at, 
      ip_address_hash,
      {{ dbt_utils.star(from=ref('version_usage_data_source'), relation_alias='usage_data', except=['EDITION', 'CREATED_AT', 'SOURCE_IP']) }},
      cleaned_edition AS edition, 
      IFF(cleaned_edition = 'CE', 'CE', 'EE')                                                  AS main_edition,
      CASE 
        WHEN edition = 'CE'                                     THEN 'Core'
        WHEN edition = 'EE Free'                                THEN 'Core'                                                      
        WHEN license_expires_at < ping_created_at                        THEN 'Core'
        WHEN edition = 'EE'                                     THEN 'Starter'
        WHEN edition = 'EES'                                    THEN 'Starter'
        WHEN edition = 'EEP'                                    THEN 'Premium'
        WHEN edition = 'EEU'                                    THEN 'Ultimate'
        ELSE NULL END                                                                           AS product_tier,
      main_edition || ' - ' || product_tier                                                     AS main_edition_product_tier,
      version_is_prerelease,
      major_version,
      minor_version,
      major_minor_version,
      CASE
        WHEN uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'      THEN 'SaaS'
        ELSE 'Self-Managed'
        END                                                                                     AS usage_ping_delivery_type,
      -- put that in a macro
      CASE
        WHEN usage_ping_delivery_type = 'SaaS'                  THEN TRUE 
        WHEN installation_type = 'gitlab-development-kit'       THEN TRUE 
        WHEN hostname = 'gitlab.com'                            THEN TRUE 
        WHEN hostname ILIKE '%.gitlab.com'                      THEN TRUE 
        ELSE FALSE END                                                                          AS is_internal, 
      -- put that in a macro
      CASE
        WHEN hostname ilike 'staging.%'                         THEN TRUE
        WHEN hostname IN ( 
        'staging.gitlab.com',
        'dr.gitlab.com'
      )                                                         THEN TRUE
        ELSE FALSE END                                                                          AS is_staging,     
      COALESCE(raw_usage_data.raw_usage_data_payload, raw_usage_data_payload_reconstructed)     AS raw_usage_data_payload,
      prep_license.dim_license_id,
      prep_subscription.dim_subscription_id,
      dim_date.date_id,
      TO_DATE(raw_usage_data.raw_usage_data_payload:license_trial_ends_on::TEXT)                      AS license_trial_ends_on,
      (raw_usage_data.raw_usage_data_payload:license_subscription_id::TEXT)                           AS license_subscription_id,
      raw_usage_data.raw_usage_data_payload:usage_activity_by_stage_monthly.manage.events::NUMBER     AS umau_value,
      IFF(ping_created_at < license_trial_ends_on, TRUE, FALSE)                        AS is_trial

    FROM usage_data
    LEFT JOIN raw_usage_data
      ON usage_data.raw_usage_data_id = raw_usage_data.raw_usage_data_id
    LEFT JOIN prep_license
      ON usage_data.license_md5 = prep_license.license_md5
    LEFT JOIN prep_subscription
      ON prep_license.dim_subscription_id = prep_subscription.dim_subscription_id
    LEFT JOIN dim_date 
      ON TO_DATE(ping_created_at) = dim_date.date_day
), map_ip_location AS (

    SELECT 
      map_ip_to_country.ip_address_hash, 
      map_ip_to_country.dim_location_country_id  
    FROM map_ip_to_country
    INNER JOIN locations 
      WHERE map_ip_to_country.dim_location_country_id = locations.dim_location_country_id

), add_country_info_to_usage_ping AS (

    SELECT 
      joined.*, 
      map_ip_location.dim_location_country_id 
    FROM joined 
    LEFT JOIN map_ip_location
      ON joined.ip_address_hash = map_ip_location.ip_address_hash 

), dim_product_tier AS (
  
    SELECT * 
    FROM {{ ref('dim_product_tier') }}
    WHERE product_delivery_type = 'Self-Managed'

), final AS (

    SELECT 
      add_country_info_to_usage_ping.dim_usage_ping_id,
      dim_product_tier.dim_product_tier_id AS dim_product_tier_id,
      dim_subscription_id,
      dim_license_id,
      dim_location_country_id,
      date_id AS dim_date_id,
      uuid AS dim_instance_id,

      -- timestamps
      ping_created_at,
      DATEADD('days', -28, ping_created_at)              AS ping_created_at_28_days_earlier,
      DATE_TRUNC('YEAR', ping_created_at)                AS ping_created_at_year,
      DATE_TRUNC('MONTH', ping_created_at)               AS ping_created_at_month,
      DATE_TRUNC('WEEK', ping_created_at)                AS ping_created_at_week,
      DATE_TRUNC('DAY', ping_created_at)                 AS ping_created_at_date,
      raw_usage_data_id                                  AS raw_usage_data_id, 
      
      -- metadata
      raw_usage_data_payload, 
      main_edition, 
      product_tier, 
      main_edition_product_tier, 
      version_is_prerelease,
      major_version,
      minor_version,
      major_minor_version,
      usage_ping_delivery_type,
      is_internal, 
      is_staging, 
      is_trial,
      instance_user_count,
      hostname AS host_name
    FROM add_country_info_to_usage_ping
    LEFT JOIN dim_product_tier
      ON TRIM(LOWER(add_country_info_to_usage_ping.product_tier)) = TRIM(LOWER(dim_product_tier.product_tier_historical_short))
      AND main_edition = 'EE'

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mpeychet",
    updated_by="@mpeychet",
    created_date="2021-01-10",
    updated_date="2021-04-30"
) }}
