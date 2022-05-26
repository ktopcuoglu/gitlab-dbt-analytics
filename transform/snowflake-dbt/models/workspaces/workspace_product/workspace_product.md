{% docs rpt_ping_instance_metric_adoption_monthly_all %}


Type of Data: Version app
Aggregate Grain: reporting_month, metrics_path, and estimation_grain
Time Grain: None
Use case: Model used to determine active seats and subscriptions reporting on any given metric


{% enddocs %}

{% docs rpt_ping_instance_metric_adoption_subscription_monthly %}

Model used to determine active seats and subscriptions reporting on any given metric

{% enddocs %}


{% docs rpt_ping_instance_metric_adoption_subscription_metric_monthly %}

Model used to determine active seats and subscriptions reporting on any given metric

{% enddocs %}


{% docs rpt_ping_instance_metric_estimated_monthly %}

Type of Data: Version app
Aggregate Grain: reporting_month, metrics_path, estimation_grain, ping_edition_product_tier, and service_ping_delivery_type
Time Grain: None
Use case: Model used to estimate usage based upon reported and unreported seats/subscriptions for any given metric.

{% enddocs %}

{% docs mart_ping_estimations_monthly %}

Estimation model to estimate the usage for unreported self-managed instances.

{% enddocs %}

{% docs rpt_ping_counter_statistics %}

**Description:**  Metrics with First/Last Verisons and Installation Counts
- This table provides First and Last Application Software Versions along with the Count of Installations for each grouping (Metric, Ping Edition and Prerelease).    

**Data Grain:**
- metrics_path
- ping_edition
- version_is_prerelease

**Filters:**
- Include Valid Major_Minor_Versions
- Exclude GitLab's Service Ping Metrics
- Include metrics for 28 Day and ALL timeframes
- Include metrics only from the `Last Pings of the Month`
- Include metrics where `has_timed_out' = FALSE (to remove Pings that have timed out during processing and may have imcomplete data)
- Include `metric_value' IS NOT NULL - ???
- Include `metric_value` >= 0 (-1 means timed out and -1000 means ???)

**Business Logic in this Model:** 
- Metrics in this data comes from a distinct list of Metrics in the [mart_ping_instance_metric_monthly](https://gitlab-data.gitlab.io/analytics/#!/model/model.gitlab_snowflake.mart_ping_instance_metric_monthly) table.  
- First and Last Versions are picked for each group (metrics_path, ping_edition, version_is_prerelease)  
- Valid Versions are those that Exists in [dim_gitlab_releases](https://gitlab-data.gitlab.io/analytics/#!/model/model.gitlab_snowflake.dim_gitlab_releases)
- GitLab's Service Ping data - where dim_intance_id = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'
- The Last Ping of the Month - ???     

**Other Comments:**
- Service Ping data is Sums, Counts and Percents of Usage (called metrics) along wth the Server Instance Configuration information is collected at a point in time for each Instance and sent to GitLab Corporate.  This is normally done on a weekly basis.  The Instance Owner determines whether this data will be sent or not and how much will be sent.  Implementations can be Customer Hosted (Self-Managed) or GitLab Hosted (referred to as SaaS or Dotcom data).  Multiple Instances can be hosted on Self-Managed Implementations like GitLab Implementations. 
- The different types of Service Pings are shown here for the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping) and the [GitLab Hosted Implementation Service Pings](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs rpt_ping_instance_subscription_opt_in_monthly %}

Monthly counts of active subscriptions.

{% enddocs %}

{% docs rpt_ping_instance_subscription_metric_opt_in_monthly %}

Monthly counts of active subscriptions.

{% enddocs %}

{% docs rpt_ping_instance_active_subscriptions %}

**Description:**  Service Ping Instance data by Installation and Month with Subscriptions, ARR and Ping Counts
- The data is Aggregated from Service Ping Instance data by Installation and Month.  And includes Latest Subscription, Version, ARR, MRR and Ping Count information. 

**Data Grain:**
- ping_created_at_month
- lastest_active_subscription_id
- dim_installation_id
- ping_edition
- version_is_prerelease

**Filters:**
- Include Valid Major_Minor_Versions
- Exclude GitLab Instance data
- Include metrics for 28 Day and ALL timeframes
- Include metrics only from the `Last Pings of the Month`
- Include metrics where `has_timed_out' = FALSE (to remove Pings that have timed out during processing and may have imcomplete data)
- Include `metric_value` IS NOT NULL - ???
- Include `metric_value` >= 0 (-1 means record timed out during data load and -1000 means the Metric is Depracated)

**Business Logic in this Model:** 
- Metrics in this data comes from a distinct list of Metrics in the [mart_ping_instance_metric_monthly](https://gitlab-data.gitlab.io/analytics/#!/model/model.gitlab_snowflake.mart_ping_instance_metric_monthly) table.  
- First and Last Major and Minor Versions are picked for each group (metrics_path, ping_edition, version_is_prerelease)  
- Valid Versions - where major_minor_version Exists in [dim_gitlab_releases](https://gitlab-data.gitlab.io/analytics/#!/model/model.gitlab_snowflake.dim_gitlab_releases)
- GitLab Instance - where dim_intance_id = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'
- The Last Ping of the Month - ??? 
- MRR, ARR and Licensed_User_Count is limited to:
  - product_delivery_type = `Self-Managed` 
  - subscription_status IN (`Active`,`Cancelled`)
  - product_tier_name <> `Storage`
- The Lastest Subscription from the Installation with the Latest Subscription in the Instance is used    

**Other Comments:**
- Service Ping data is Sums, Counts and Percents of Usage (called metrics) along wth the Server Instance Configuration information is collected at a point in time for each Instance and sent to GitLab Corporate.  This is normally done on a weekly basis.  The Instance Owner determines whether this data will be sent or not and how much will be sent.  Implementations can be Customer Hosted (Self-Managed) or GitLab Hosted (referred to as SaaS or Dotcom data).  Multiple Instances can be hosted on Self-Managed Implementations like GitLab Implementations. 
- The different types of Service Pings are shown here for the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping) and the [GitLab Hosted Implementation Service Pings](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}
