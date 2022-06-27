{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ simple_cte([
    ('metric_opt_in', 'rpt_ping_metric_first_last_versions'),
    ('mart_charge', 'mart_charge'),
    ('dim_charge', 'dim_charge'),
    ('fct_charge', 'fct_charge'),
    ('dim_subscription', 'dim_subscription'),
    ('dim_billing_account', 'dim_billing_account'),
    ('dim_crm_account', 'dim_crm_account'),
    ('dim_product_detail', 'dim_product_detail'),
    ('dim_date', 'dim_date'),
    ('mart_ping_instance_metric', 'mart_ping_instance_metric'),
    ('mart_ping_instance_metric_monthly', 'mart_ping_instance_metric_monthly')
    ])

}}

/*
Determine latest version for each subscription to determine if the potential metric is valid for a given month
*/

, subscriptions_w_versions AS (

  SELECT
      ping_created_at_month             AS ping_created_at_month,
      dim_installation_id               AS dim_installation_id,
      latest_subscription_id            AS latest_subscription_id,
      ping_edition                      AS ping_edition,
      version_is_prerelease             AS version_is_prerelease,
      major_minor_version_id            AS major_minor_version_id,
      instance_user_count               AS instance_user_count
  FROM mart_ping_instance_metric_monthly
      WHERE ping_delivery_type = 'Self-Managed'
      QUALIFY ROW_NUMBER() OVER (
            PARTITION BY ping_created_at_month, latest_subscription_id, dim_installation_id
              ORDER BY major_minor_version_id DESC) = 1

/*
Deduping the mart to ensure instance_user_count isn't counted 2+ times
*/

), deduped_subscriptions_w_versions AS (

    SELECT
        ping_created_at_month             AS ping_created_at_month,
        dim_installation_id               AS dim_installation_id,
        latest_subscription_id            AS latest_subscription_id,
        ping_edition                      AS ping_edition,
        version_is_prerelease             AS version_is_prerelease,
        major_minor_version_id            AS major_minor_version_id,
        MAX(instance_user_count)          AS instance_user_count
    FROM subscriptions_w_versions
      {{ dbt_utils.group_by(n=6)}}
/*
Get the count of pings each month per subscription_name_slugify
*/

), ping_counts AS (

  SELECT
    ping_created_at_month                       AS ping_created_at_month,
    dim_installation_id                         AS dim_installation_id,
    latest_subscription_id                      AS latest_subscription_id,
    COUNT(DISTINCT(dim_ping_instance_id))       AS ping_count
  FROM mart_ping_instance_metric
      {{ dbt_utils.group_by(n=3)}}

/*
Join subscription information with count of pings
*/

), joined_subscriptions AS (

  SELECT
    deduped_subscriptions_w_versions.*,
    ping_counts.ping_count
  FROM deduped_subscriptions_w_versions
    INNER JOIN ping_counts
  ON deduped_subscriptions_w_versions.ping_created_at_month = ping_counts.ping_created_at_month
    AND deduped_subscriptions_w_versions.latest_subscription_id = ping_counts.latest_subscription_id
    AND deduped_subscriptions_w_versions.dim_installation_id = ping_counts.dim_installation_id
/*
Aggregate mart_charge information (used as the basis of truth), this gets rid of host deviation
*/

), mart_charge_cleaned AS (

  SELECT
       dim_date.date_actual               AS arr_month,
       fct_charge.dim_subscription_id     AS dim_subscription_id,
       SUM(mrr)                           AS mrr,
       SUM(arr)                           AS arr,
       SUM(quantity)                      AS licensed_user_count
     FROM fct_charge
     INNER JOIN dim_date
        ON effective_start_month <= dim_date.date_actual
        AND (effective_end_month >= dim_date.date_actual OR effective_end_month IS NULL)
        AND dim_date.day_of_month = 1
     INNER JOIN dim_charge
       ON fct_charge.dim_charge_id = dim_charge.dim_charge_id
     INNER JOIN dim_subscription
       ON fct_charge.dim_subscription_id = dim_subscription.dim_subscription_id
     INNER JOIN dim_product_detail
       ON fct_charge.dim_product_detail_id = dim_product_detail.dim_product_detail_id
      WHERE dim_product_detail.product_delivery_type = 'Self-Managed'
        AND subscription_status IN ('Active','Cancelled')
        AND dim_product_detail.product_tier_name != 'Storage'
        AND DATE_TRUNC('MONTH', CURRENT_DATE) > arr_month
      {{ dbt_utils.group_by(n=2)}}

/*
Join mart_charge information bringing in mart_charge subscriptions which DO NOT appear in ping fact data
*/

), arr_counts_joined AS (

  SELECT
    mart_charge_cleaned.arr_month                                                                           AS ping_created_at_month,
    joined_subscriptions.dim_installation_id                                                                AS dim_installation_id,
    mart_charge_cleaned.dim_subscription_id                                                                 AS latest_subscription_id,
    joined_subscriptions.ping_edition                                                                       AS ping_edition,
    joined_subscriptions.version_is_prerelease                                                              AS version_is_prerelease,
    joined_subscriptions.major_minor_version_id                                                             AS major_minor_version_id,
    joined_subscriptions.instance_user_count                                                                AS instance_user_count,
    mart_charge_cleaned.licensed_user_count                                                                 AS licensed_user_count,
    mart_charge_cleaned.arr                                                                                 AS arr,
    mart_charge_cleaned.mrr                                                                                 AS mrr,
    joined_subscriptions.ping_count                                                                         AS ping_count,
    FALSE                                                                                                   AS is_missing_charge_subscription
  FROM mart_charge_cleaned
    LEFT OUTER JOIN joined_subscriptions
  ON joined_subscriptions.latest_subscription_id = mart_charge_cleaned.dim_subscription_id
      AND joined_subscriptions.ping_created_at_month = mart_charge_cleaned.arr_month

/*
Grab the latest values to join to missing subs
*/

), latest_mart_charge_values AS (

    SELECT
        dim_subscription_id,
        mrr,
        arr,
        licensed_user_count
    FROM mart_charge_cleaned
        QUALIFY ROW_NUMBER() OVER (
              PARTITION BY dim_subscription_id
              ORDER BY arr_month DESC) = 1

/*
This CTE below grabs the missing installation/subs for each month missing from arr_counts_joined (latest_subs) where there are actual pings from that install/sub combo)
*/

), missing_subs AS (

    SELECT
        ping_created_at_month                   AS ping_created_at_month,
        dim_installation_id                     AS dim_installation_id,
        latest_subscription_id                  AS latest_subscription_id,
        ping_edition                            AS ping_edition,
        version_is_prerelease                   AS version_is_prerelease,
        MAX(major_minor_version_id)             AS major_minor_version_id,
        MAX(instance_user_count)                AS instance_user_count,
        COUNT(DISTINCT(dim_ping_instance_id))   AS ping_count
    FROM mart_ping_instance_metric
        WHERE is_last_ping_of_month = TRUE
          AND CONCAT(latest_subscription_id, to_varchar(ping_created_at_month)) NOT IN
            (SELECT DISTINCT(CONCAT(latest_subscription_id, to_varchar(ping_created_at_month))) FROM arr_counts_joined)
          {{ dbt_utils.group_by(n=5)}}

/*
Join to capture missing metrics, uses the last value found for these in fct_charge
*/

), missing_subs_joined AS (

    SELECT
        missing_subs.*,
        latest_mart_charge_values.licensed_user_count         AS licensed_user_count,
        latest_mart_charge_values.arr                         AS arr,
        latest_mart_charge_values.mrr                         AS mrr,
        TRUE                                                  AS is_missing_charge_subscription
    FROM missing_subs
        INNER JOIN latest_mart_charge_values
    ON missing_subs.latest_subscription_id = latest_mart_charge_values.dim_subscription_id

), latest_subs_unioned AS (

    SELECT
        ping_created_at_month,
        dim_installation_id,
        latest_subscription_id,
        ping_edition,
        version_is_prerelease,
        major_minor_version_id,
        instance_user_count,
        licensed_user_count,
        arr,
        mrr,
        ping_count,
        is_missing_charge_subscription
    FROM arr_counts_joined

        UNION ALL

    SELECT
        ping_created_at_month,
        dim_installation_id,
        latest_subscription_id,
        ping_edition,
        version_is_prerelease,
        major_minor_version_id,
        instance_user_count,
        licensed_user_count,
        arr,
        mrr,
        ping_count,
        is_missing_charge_subscription
    FROM missing_subs_joined

), final AS (

    SELECT
        {{ dbt_utils.surrogate_key(['ping_created_at_month', 'latest_subscription_id', 'dim_installation_id', 'ping_edition', 'version_is_prerelease']) }}                        AS ping_latest_subscriptions_monthly_id,
        latest_subs_unioned.ping_created_at_month                                                                                                                                 AS ping_created_at_month,
        latest_subs_unioned.dim_installation_id                                                                                                                                   AS dim_installation_id,
        latest_subs_unioned.latest_subscription_id                                                                                                                                AS latest_subscription_id,
        latest_subs_unioned.ping_edition                                                                                                                                          AS ping_edition,
        latest_subs_unioned.version_is_prerelease                                                                                                                                 AS version_is_prerelease,
        latest_subs_unioned.major_minor_version_id                                                                                                                                AS major_minor_version_id,
        latest_subs_unioned.instance_user_count                                                                                                                                   AS instance_user_count,
        FLOOR(latest_subs_unioned.licensed_user_count)                                                                                                                            AS licensed_user_count,
        latest_subs_unioned.arr                                                                                                                                                   AS arr,
        latest_subs_unioned.mrr                                                                                                                                                   As mrr,
        IFNULL(latest_subs_unioned.ping_count, 0)                                                                                                                                 AS ping_count,
        IFF(latest_subs_unioned.ping_edition IS NULL, FALSE, TRUE)                                                                                                                AS has_sent_pings,
        latest_subs_unioned.is_missing_charge_subscription                                                                                                                        AS is_missing_charge_subscription
    FROM latest_subs_unioned
      WHERE ping_created_at_month < DATE_TRUNC('month', CURRENT_DATE)

)

 {{ dbt_audit(
     cte_ref="final",
     created_by="@icooper-acp",
     updated_by="@snalamaru",
     created_date="2022-05-05",
     updated_date="2022-06-07"
 ) }}
