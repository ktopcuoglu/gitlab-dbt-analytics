{{config({
    "schema": "common_mart_product"
  })
}}

{{ simple_cte([
    ('monthly_metrics','fct_product_usage_wave_1_3_metrics_monthly'),
    ('dim_date','dim_date')
]) }}

, months AS (

    SELECT DISTINCT
      first_day_of_month,
      days_in_month_count
    FROM dim_date

), ping_ranges AS (

    SELECT DISTINCT
      dim_subscription_id,
      MIN(IFF(commit_comment_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS commit_comment_first_ping_month,
      MAX(IFF(commit_comment_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS commit_comment_last_ping_month,  
      MIN(IFF(source_code_pushes_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS source_code_pushes_first_ping_month,
      MAX(IFF(source_code_pushes_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS source_code_pushes_last_ping_month,  
      MIN(IFF(ci_builds_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS ci_builds_first_ping_month,
      MAX(IFF(ci_builds_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS ci_builds_last_ping_month,  
      MIN(IFF(ci_builds_all_time_user IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS ci_build_users_first_ping_month,
      MAX(IFF(ci_builds_all_time_user IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS ci_build_users_event_last_ping_month,   
      MIN(IFF(ci_runners_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS ci_runners_first_ping_month,
      MAX(IFF(ci_runners_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS ci_runners_last_ping_month,  
      MIN(IFF(template_repositories_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS template_repositories_first_ping_month,
      MAX(IFF(template_repositories_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS template_repositories_last_ping_month,  
      MIN(IFF(projects_with_packages_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS projects_with_packages_first_ping_month,
      MAX(IFF(projects_with_packages_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS projects_with_packages_last_ping_month,  
      MIN(IFF(auto_devops_enabled_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS auto_devops_enabled_first_ping_month,
      MAX(IFF(auto_devops_enabled_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS auto_devops_enabled_last_ping_month  
    FROM monthly_metrics

), diffs AS (

    SELECT
      dim_subscription_id,
      dim_subscription_id_original,
      dim_billing_account_id,
      snapshot_month,
      uuid,
      hostname,
      ping_created_at::DATE - LAG(ping_created_at::DATE)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS days_since_last_ping,
      commit_comment_all_time_event,
      commit_comment_all_time_event - LAG(commit_comment_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS commit_comment_since_last_ping,
      source_code_pushes_all_time_event,
      source_code_pushes_all_time_event - LAG(source_code_pushes_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS source_code_pushes_since_last_ping,
      ci_builds_all_time_event,
      ci_builds_all_time_event - LAG(ci_builds_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS ci_builds_since_last_ping,
      ci_runners_all_time_event,
      ci_runners_all_time_event - LAG(ci_runners_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS ci_runners_since_last_ping,
      template_repositories_all_time_event,
      template_repositories_all_time_event - LAG(template_repositories_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS template_repositories_since_last_ping,
      projects_with_packages_all_time_event,
      projects_with_packages_all_time_event - LAG(projects_with_packages_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS projects_with_packages_since_last_ping,
      auto_devops_enabled_all_time_event,
      auto_devops_enabled_all_time_event - LAG(auto_devops_enabled_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS auto_devops_enabled_since_last_ping,
      ci_builds_all_time_user,
      ci_builds_all_time_user - LAG(ci_builds_all_time_user)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS ci_build_users_since_last_ping
    FROM monthly_metrics

), smoothed_diffs AS (

    SELECT
      dim_subscription_id,
      dim_subscription_id_original,
      dim_billing_account_id,
      snapshot_month,
      uuid,
      hostname,
      commit_comment_all_time_event,
      commit_comment_since_last_ping,
      FIRST_VALUE(commit_comment_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS commit_comment_per_day,
      (commit_comment_per_day * days_in_month_count)::INT                               AS commit_comment_smoothed,
      source_code_pushes_all_time_event,
      source_code_pushes_since_last_ping,
      FIRST_VALUE(source_code_pushes_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS source_code_pushes_per_day,
      (source_code_pushes_per_day * days_in_month_count)::INT                           AS source_code_pushes_smoothed,
      ci_builds_all_time_event,
      ci_builds_since_last_ping,
      FIRST_VALUE(ci_builds_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS ci_builds_per_day,
      (ci_builds_per_day * days_in_month_count)::INT                                    AS ci_builds_smoothed,
      ci_runners_all_time_event,
      ci_runners_since_last_ping,
      FIRST_VALUE(ci_runners_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS ci_runners_per_day,
      (ci_runners_per_day * days_in_month_count)::INT                                   AS ci_runners_smoothed,
      template_repositories_all_time_event,
      template_repositories_since_last_ping,
      FIRST_VALUE(template_repositories_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS template_repositories_per_day,
      (template_repositories_per_day * days_in_month_count)::INT                        AS template_repositories_smoothed,
      projects_with_packages_all_time_event,
      projects_with_packages_since_last_ping,
      FIRST_VALUE(projects_with_packages_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS projects_with_packages_per_day,
      (projects_with_packages_per_day * days_in_month_count)::INT                       AS projects_with_packages_smoothed,
      auto_devops_enabled_all_time_event,
      auto_devops_enabled_since_last_ping,
      FIRST_VALUE(auto_devops_enabled_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS auto_devops_enabled_per_day,
      (auto_devops_enabled_per_day * days_in_month_count)::INT                          AS auto_devops_enabled_smoothed,
      ci_builds_all_time_user,
      ci_build_users_since_last_ping
    FROM diffs
    INNER JOIN months
      ON diffs.snapshot_month = months.first_day_of_month

), final AS (

    SELECT
      smoothed_diffs.dim_subscription_id,
      dim_subscription_id_original,
      dim_billing_account_id,
      snapshot_month,
      uuid,
      hostname,
      commit_comment_all_time_event,
      commit_comment_since_last_ping,
      IFF(snapshot_month <= commit_comment_first_ping_month
            OR snapshot_month > commit_comment_last_ping_month,
          NULL, commit_comment_smoothed)                                                AS commit_comment_estimated_monthly,
      source_code_pushes_all_time_event,
      source_code_pushes_since_last_ping,
      IFF(snapshot_month <= source_code_pushes_first_ping_month
            OR snapshot_month > source_code_pushes_last_ping_month,
          NULL, source_code_pushes_smoothed)                                            AS source_code_pushes_estimated_monthly,
      ci_builds_all_time_event,
      ci_builds_since_last_ping,
      IFF(snapshot_month <= ci_builds_first_ping_month
            OR snapshot_month > ci_builds_last_ping_month,
          NULL, ci_builds_smoothed)                                                     AS ci_builds_estimated_monthly,
      ci_runners_all_time_event,
      ci_runners_since_last_ping,
      IFF(snapshot_month <= ci_runners_first_ping_month
            OR snapshot_month > ci_runners_last_ping_month,
          NULL, ci_runners_smoothed)                                                    AS ci_runners_estimated_monthly,
      template_repositories_all_time_event,
      template_repositories_since_last_ping,
      IFF(snapshot_month <= template_repositories_first_ping_month
            OR snapshot_month > template_repositories_last_ping_month,
          NULL, template_repositories_smoothed)                                         AS template_repositories_estimated_monthly,
      projects_with_packages_all_time_event,
      projects_with_packages_since_last_ping,
      IFF(snapshot_month <= projects_with_packages_first_ping_month
            OR snapshot_month > projects_with_packages_last_ping_month,
          NULL, projects_with_packages_smoothed)                                        AS projects_with_packages_estimated_monthly,
      auto_devops_enabled_all_time_event,
      auto_devops_enabled_since_last_ping,
      IFF(snapshot_month <= auto_devops_enabled_first_ping_month
            OR snapshot_month > auto_devops_enabled_last_ping_month,
          NULL, auto_devops_enabled_smoothed)                                           AS auto_devops_enabled_estimated_monthly,
      ci_builds_all_time_user,
      ci_build_users_since_last_ping
    FROM smoothed_diffs
    LEFT JOIN ping_ranges
      ON smoothed_diffs.dim_subscription_id = ping_ranges.dim_subscription_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ischweickartDD",
    updated_by="@vedprakash2021",
    created_date="2021-03-04",
    updated_date="2021-03-16"
) }}