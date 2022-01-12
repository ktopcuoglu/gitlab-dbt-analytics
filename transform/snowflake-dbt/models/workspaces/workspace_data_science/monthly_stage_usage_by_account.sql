{{ config(
     materialized = "table"
) }}

WITH usage_ping AS (
    SELECT
        *
    FROM {{ ref('prep_usage_ping') }}
),

license_subscription_mapping AS (
    SELECT
        *
    FROM {{ ref('map_license_subscription_account') }}
),

dates AS (
    SELECT
        *
    FROM {{ ref('dim_date') }}
),

saas_usage_ping AS (
    SELECT
        *
    FROM {{ ref('prep_saas_usage_ping_namespace') }}
),

namespace_subscription_bridge AS (
    SELECT
        *
    FROM {{ ref('bdg_namespace_order_subscription_monthly') }}
),

usage_ping_metrics AS (
    SELECT
        *
    FROM {{ ref('dim_usage_ping_metric') }}
),

sm_last_monthly_ping_per_account AS (
    SELECT
        license_subscription_mapping.dim_crm_account_id,
        license_subscription_mapping.dim_subscription_id,
        usage_ping.dim_instance_id AS uuid,
        usage_ping.host_name AS hostname,
        usage_ping.raw_usage_data_payload,
        CAST(usage_ping.ping_created_at_month AS DATE) AS snapshot_month
    FROM usage_ping
    LEFT JOIN
        license_subscription_mapping ON
            usage_ping.license_md5 = REPLACE(
                license_subscription_mapping.license_md5, '-'
            )
    WHERE usage_ping.license_md5 IS NOT NULL
        AND CAST(
            usage_ping.ping_created_at_month AS DATE
        ) < DATE_TRUNC('month', CURRENT_DATE)
  QUALIFY ROW_NUMBER () OVER (
    PARTITION BY
      license_subscription_mapping.dim_subscription_id,
      usage_ping.dim_instance_id,
      usage_ping.host_name,
      CAST(usage_ping.ping_created_at_month AS DATE)
    ORDER BY
      license_subscription_mapping.dim_subscription_id,
      usage_ping.dim_instance_id,
      usage_ping.host_name,
      usage_ping.ping_created_at DESC
  ) = 1
),

saas_last_monthly_ping_per_account AS (
    SELECT
        namespace_subscription_bridge.dim_crm_account_id,
        namespace_subscription_bridge.dim_subscription_id,
        namespace_subscription_bridge.dim_namespace_id,
        namespace_subscription_bridge.snapshot_month,
        saas_usage_ping.ping_name AS metrics_path,
        saas_usage_ping.counter_value AS metrics_value
    FROM saas_usage_ping
    INNER JOIN dates ON saas_usage_ping.ping_date = dates.date_day
    INNER JOIN
        namespace_subscription_bridge ON
            saas_usage_ping.dim_namespace_id =
            namespace_subscription_bridge.dim_namespace_id
            AND dates.first_day_of_month =
            namespace_subscription_bridge.snapshot_month
            AND namespace_subscription_bridge.namespace_order_subscription_match_status = 'Paid All Matching'
    WHERE namespace_subscription_bridge.dim_crm_account_id IS NOT NULL
        AND namespace_subscription_bridge.snapshot_month < DATE_TRUNC(
            'month', CURRENT_DATE
        )
        AND metrics_path LIKE 'usage_activity_by_stage%'
        AND metrics_value > 0 -- Filter out non-instances
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      namespace_subscription_bridge.dim_subscription_id,
      namespace_subscription_bridge.dim_namespace_id,
      namespace_subscription_bridge.snapshot_month,
      saas_usage_ping.ping_name
    ORDER BY
      namespace_subscription_bridge.dim_subscription_id,
      namespace_subscription_bridge.dim_namespace_id,
      saas_usage_ping.ping_date DESC
  ) = 1
),

flattened_metrics AS (
    SELECT
        dim_crm_account_id,
        dim_subscription_id,
        NULL AS dim_namespace_id,
        uuid,
        hostname,
        snapshot_month,
        "PATH" AS metrics_path,
        "VALUE" AS metrics_value
    FROM sm_last_monthly_ping_per_account,
        LATERAL(INPUT => raw_usage_data_payload, RECURSIVE => TRUE)
    WHERE metrics_path LIKE 'usage_activity_by_stage%'
        AND IS_REAL(metrics_value) = 1
        AND metrics_value > 0

    UNION ALL

    SELECT
        dim_crm_account_id,
        dim_subscription_id,
        dim_namespace_id,
        NULL AS uuid,
        NULL AS hostname,
        snapshot_month,
        metrics_path,
        metrics_value
    FROM saas_last_monthly_ping_per_account
)

SELECT
    flattened_metrics.dim_crm_account_id,
    flattened_metrics.snapshot_month,

    -- NUMBER OF FEATURES USED BY PRODUCT STAGE
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage = 'plan'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_plan_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage = 'plan'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_plan_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage IN (
                    'create', 'devops::create'
                ) AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_create_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage IN (
                    'create', 'devops::create'
                ) AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_create_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage = 'verify'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_verify_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage = 'verify'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_verify_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage = 'package'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_package_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage = 'package'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_package_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage IN (
                    'release', 'releases'
                ) AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_release_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage IN (
                    'release', 'releases'
                ) AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_release_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage = 'configure'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_configure_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage = 'configure'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_configure_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage = 'monitor'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_monitor_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage = 'monitor'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_monitor_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage IN (
                    'manage', 'managed'
                ) AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_manage_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage IN (
                    'manage', 'managed'
                ) AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_manage_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage IN (
                    'secure', 'devops::secure'
                ) AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_secure_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage IN (
                    'secure', 'devops::secure'
                ) AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_secure_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage = 'protect'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_protect_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage = 'protect'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_protect_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage = 'ecosystem'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_ecosystem_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage = 'ecosystem'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_ecosystem_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage = 'growth'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_growth_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage = 'growth'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_growth_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage = 'enablement'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_enablement_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_stage = 'enablement'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_enablement_28days_features,

    -- NUMBER OF FEATURES USED BY PRODUCT STAGE
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_section = 'dev'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_dev_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_section = 'dev'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_dev_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_section = 'enablement'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_enablement_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_section = 'enablement'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_enablement_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_section = 'fulfillment'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_fulfillment_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_section = 'fulfillment'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_fulfillment_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_section = 'growth'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_growth_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_section = 'growth'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_growth_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_section = 'ops'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_ops_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_section = 'ops'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_ops_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_section = 'sec'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_sec_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_section = 'sec'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_sec_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_section = 'seg'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_seg_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.product_section = 'seg'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_seg_28days_features,

    -- NUMBER OF FEATURES USED BY PRODUCT TIER
    COUNT(
        DISTINCT CASE
            WHEN
                CONTAINS(
                    usage_ping_metrics.tier, 'free'
                ) AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS tier_free_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                CONTAINS(
                    usage_ping_metrics.tier, 'free'
                ) AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS tier_free_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                CONTAINS(
                    usage_ping_metrics.tier, 'premium'
                ) AND NOT CONTAINS(
                    usage_ping_metrics.tier, 'free'
                ) AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS tier_premium_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                CONTAINS(
                    usage_ping_metrics.tier, 'premium'
                ) AND NOT CONTAINS(
                    usage_ping_metrics.tier, 'free'
                ) AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS tier_premium_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                CONTAINS(
                    usage_ping_metrics.tier, 'ultimate'
                ) AND NOT CONTAINS(
                    usage_ping_metrics.tier, 'premium'
                ) AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS tier_ultimate_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                CONTAINS(
                    usage_ping_metrics.tier, 'ultimate'
                ) AND NOT CONTAINS(
                    usage_ping_metrics.tier, 'premium'
                ) AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS tier_ultimate_28days_features,

    -- NUMBER OF TIMES FEAURES ARE USED BY STAGE
    COALESCE(
        SUM(
            CASE
                WHEN
                    usage_ping_metrics.product_stage = 'plan'
                    AND usage_ping_metrics.time_frame = 'all'
                    THEN flattened_metrics.metrics_value
            END
        ),
        0
    ) AS stage_plan_alltime_feature_sum,
    COALESCE(
        SUM(
            CASE
                WHEN
                    usage_ping_metrics.product_stage IN (
                        'create', 'devops::create'
                    ) AND usage_ping_metrics.time_frame = 'all'
                    THEN flattened_metrics.metrics_value
            END
        ),
        0
    ) AS stage_create_alltime_feature_sum,
    COALESCE(
        SUM(
            CASE
                WHEN
                    usage_ping_metrics.product_stage = 'verify'
                    AND usage_ping_metrics.time_frame = 'all'
                    THEN flattened_metrics.metrics_value
            END
        ),
        0
    ) AS stage_verify_alltime_feature_sum,
    COALESCE(
        SUM(
            CASE
                WHEN
                    usage_ping_metrics.product_stage = 'package'
                    AND usage_ping_metrics.time_frame = 'all'
                    THEN flattened_metrics.metrics_value
            END
        ),
        0
    ) AS stage_package_alltime_feature_sum,
    COALESCE(
        SUM(
            CASE
                WHEN
                    usage_ping_metrics.product_stage IN (
                        'release', 'releases'
                    ) AND usage_ping_metrics.time_frame = 'all'
                    THEN flattened_metrics.metrics_value
            END
        ),
        0
    ) AS stage_release_alltime_feature_sum,
    COALESCE(
        SUM(
            CASE
                WHEN
                    usage_ping_metrics.product_stage = 'configure'
                    AND usage_ping_metrics.time_frame = 'all'
                    THEN flattened_metrics.metrics_value
            END
        ),
        0
    ) AS stage_configure_alltime_features_sum,
    COALESCE(
        SUM(
            CASE
                WHEN
                    usage_ping_metrics.product_stage = 'monitor'
                    AND usage_ping_metrics.time_frame = 'all'
                    THEN flattened_metrics.metrics_value
            END
        ),
        0
    ) AS stage_monitor_alltime_features_sum,
    COALESCE(
        SUM(
            CASE
                WHEN
                    usage_ping_metrics.product_stage IN (
                        'manage', 'managed'
                    ) AND usage_ping_metrics.time_frame = 'all'
                    THEN flattened_metrics.metrics_value
            END
        ),
        0
    ) AS stage_manage_alltime_feature_sum,
    COALESCE(
        SUM(
            CASE
                WHEN
                    usage_ping_metrics.product_stage IN (
                        'secure', 'devops::secure'
                    ) AND usage_ping_metrics.time_frame = 'all'
                    THEN flattened_metrics.metrics_value
            END
        ),
        0
    ) AS stage_secure_alltime_feature_sum,
    COALESCE(
        SUM(
            CASE
                WHEN
                    usage_ping_metrics.product_stage = 'protect'
                    AND usage_ping_metrics.time_frame = 'all'
                    THEN flattened_metrics.metrics_value
            END
        ),
        0
    ) AS stage_protect_alltime_feature_sum,
    COALESCE(
        SUM(
            CASE
                WHEN
                    usage_ping_metrics.product_stage = 'ecosystem'
                    AND usage_ping_metrics.time_frame = 'all'
                    THEN flattened_metrics.metrics_value
            END
        ),
        0
    ) AS stage_ecosystem_alltime_feature_sum,
    COALESCE(
        SUM(
            CASE
                WHEN
                    usage_ping_metrics.product_stage = 'growth'
                    AND usage_ping_metrics.time_frame = 'all'
                    THEN flattened_metrics.metrics_value
            END
        ),
        0
    ) AS stage_growth_alltime_feature_sum,
    COALESCE(
        SUM(
            CASE
                WHEN
                    usage_ping_metrics.product_stage = 'enablement'
                    AND usage_ping_metrics.time_frame = 'all'
                    THEN flattened_metrics.metrics_value
            END
        ),
        0
    ) AS stage_enablement_alltime_feature_sum,

    /* If want to calculate 28 day metrics, could use the lag function. Or
       compute by nesting this SELECT statement in a WITH and computing after
       the fact, STAGE_PLAN_ALLTIME_FEATURE_SUM -
       COALESCE(LAG(STAGE_PLAN_ALLTIME_FEATURE_SUM)
       OVER (PARTITION BY flattened_metrics.DIM_CRM_ACCOUNT_ID ORDER BY
       flattened_metrics.SNAPSHOT_MONTH), 0) as STAGE_PLAN_28DAYS_FEATURE_SUM
    */

    -- FEATURE USE SHARE BY STAGE
    SUM(
        CASE
            WHEN
                usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_value
        END
    ) AS all_stages_alltime_feature_sum,
    ROUND(
        DIV0(stage_plan_alltime_feature_sum,
             all_stages_alltime_feature_sum), 4
    ) AS stage_plan_alltime_share_pct,
    ROUND(
        DIV0(stage_create_alltime_feature_sum,
             all_stages_alltime_feature_sum), 4
    ) AS stage_create_alltime_share_pct,
    ROUND(
        DIV0(stage_verify_alltime_feature_sum,
             all_stages_alltime_feature_sum), 4
    ) AS stage_verify_alltime_share_pct,
    ROUND(
        DIV0(stage_package_alltime_feature_sum,
             all_stages_alltime_feature_sum), 4
    ) AS stage_package_alltime_share_pct,
    ROUND(
        DIV0(stage_release_alltime_feature_sum,
             all_stages_alltime_feature_sum), 4
    ) AS stage_release_alltime_share_pct,
    ROUND(
        DIV0(stage_configure_alltime_features_sum,
             all_stages_alltime_feature_sum), 4
    ) AS stage_configure_alltime_share_pct,
    ROUND(
        DIV0(stage_monitor_alltime_features_sum,
             all_stages_alltime_feature_sum), 4
    ) AS stage_monitor_alltime_share_pct,
    ROUND(
        DIV0(stage_manage_alltime_feature_sum,
             all_stages_alltime_feature_sum), 4
    ) AS stage_manage_alltime_share_pct,
    ROUND(
        DIV0(stage_secure_alltime_feature_sum,
             all_stages_alltime_feature_sum), 4
    ) AS stage_secure_alltime_share_pct,
    ROUND(
        DIV0(stage_protect_alltime_feature_sum,
             all_stages_alltime_feature_sum), 4
    ) AS stage_protect_alltime_share_pct,
    ROUND(
        DIV0(stage_ecosystem_alltime_feature_sum,
             all_stages_alltime_feature_sum), 4
    ) AS stage_ecosystem_alltime_share_pct,
    ROUND(
        DIV0(stage_growth_alltime_feature_sum,
             all_stages_alltime_feature_sum), 4
    ) AS stage_growth_alltime_share_pct,
    ROUND(
        DIV0(stage_enablement_alltime_feature_sum,
             all_stages_alltime_feature_sum), 4
    ) AS stage_enablement_alltime_share_pct,

    -- MOST USED STAGE ALL TIME
    CASE GREATEST(
        stage_plan_alltime_share_pct,
        stage_create_alltime_share_pct,
        stage_verify_alltime_share_pct
    )
        WHEN stage_plan_alltime_share_pct THEN 'plan'
        WHEN stage_create_alltime_share_pct THEN 'create'
        WHEN stage_verify_alltime_share_pct THEN 'verify'
        WHEN stage_package_alltime_share_pct THEN 'package'
        WHEN stage_release_alltime_share_pct THEN 'release'
        WHEN stage_configure_alltime_share_pct THEN 'configure'
        WHEN stage_monitor_alltime_share_pct THEN 'monitor'
        WHEN stage_manage_alltime_share_pct THEN 'manage'
        WHEN stage_secure_alltime_share_pct THEN 'secure'
        WHEN stage_protect_alltime_share_pct THEN 'protect'
        WHEN stage_ecosystem_alltime_share_pct THEN 'ecosystem'
        WHEN stage_growth_alltime_share_pct THEN 'growth'
        WHEN stage_enablement_alltime_share_pct THEN 'enablement'
        ELSE 'none'
    END AS stage_most_used_alltime,


    -- NUMBER OF SEAT LICENSES USING EACH STAGE
    -- Cannot get at because of the level of granuality of the usage
    -- datflattened_metrics.

    -- TOTAL MONTHS USED BY STAGES
    CASE WHEN stage_plan_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_crm_account_id,
                    CASE WHEN stage_plan_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_plan_months_used,
    CASE WHEN stage_create_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_crm_account_id,
                    CASE WHEN stage_create_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_create_months_used,
    CASE WHEN stage_verify_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_crm_account_id,
                    CASE WHEN stage_verify_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_verify_months_used,
    CASE WHEN stage_package_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_crm_account_id,
                    CASE WHEN stage_package_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_package_months_used,
    CASE WHEN stage_release_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_crm_account_id,
                    CASE WHEN stage_release_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_release_months_used,
    CASE WHEN stage_configure_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_crm_account_id,
                    CASE WHEN stage_configure_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_configure_months_used,
    CASE WHEN stage_monitor_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_crm_account_id,
                    CASE WHEN stage_monitor_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_monitor_months_used,
    CASE WHEN stage_manage_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_crm_account_id,
                    CASE WHEN stage_manage_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_manage_months_used,
    CASE WHEN stage_secure_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_crm_account_id,
                    CASE WHEN stage_secure_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_secure_months_used,
    CASE WHEN stage_protect_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_crm_account_id,
                    CASE WHEN stage_protect_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_protect_months_used,
    CASE WHEN stage_ecosystem_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_crm_account_id,
                    CASE WHEN stage_ecosystem_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_ecosystem_months_used,
    CASE WHEN stage_growth_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_crm_account_id,
                    CASE WHEN stage_growth_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_growth_months_used,
    CASE WHEN stage_enablement_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_crm_account_id,
                    CASE WHEN stage_enablement_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_enablement_months_used

FROM flattened_metrics
LEFT JOIN
    usage_ping_metrics ON
        flattened_metrics.metrics_path = usage_ping_metrics.metrics_path
WHERE usage_ping_metrics.metrics_status = 'active'
      AND flattened_metrics.dim_crm_account_id IS NOT NULL
GROUP BY
    flattened_metrics.dim_crm_account_id,
    flattened_metrics.snapshot_month
