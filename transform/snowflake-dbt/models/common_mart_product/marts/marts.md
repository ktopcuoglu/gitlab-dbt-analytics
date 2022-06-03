{% docs mart_ping_instance_metric %}

**Description:** Atomic Level Instance Service Ping data by Instance, Host, Metric, DateTime
- Extra attributes for License, Subscription and Billing are included to allow single table queries more easily.

**Data Grain:**
- dim_instance_id
- dim_host_id
- metrics_path
- ping_created_at

**Filters:**
- none -

**Business Logic in this Model:** 
- `is_last_ping_of_month` = last ping (Instance_id and Host_id) sent for the Month
- `ping_delivery_type` = 'SaaS' WHERE UUID/Instance_id = ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f ELSE 'Self-Managed'
- `is_internal` = TRUE WHERE:
  - UUID/Instance_id = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f' 
  - (OR) installation_type = 'gitlab-development-kit'
  - (OR) hostname = 'gitlab.com' 
  - (OR) hostname LIKE '%.gitlab.com'
- `is_staging` = TRUE WHERE:
  - hostname LIKE 'staging.%'
  - (OR) hostname IN ('staging.gitlab.com','dr.gitlab.com')
- `dim_ping_instance_id` = 'source ping id' with the following grain:     
  - dim_instance_id
  - dim_host_id
  - ping_created_at
- ping_created_at- `arr` = mrr * 12
- `major_minor_version` = major_version || '.' || minor_version 
- `major_minor_version_id` = major_version * 100 + minor_version
- `version_is_prerelease` = version LIKE '%-pre'
- License / Subscription Logic:
  - `latest active subscription` WHERE subscription_status IN (`Active`,`Cancelled`)
  - `is_program_subscription` = TRUE WHERE product_rate_plan_name LIKE ('%edu%' or '%oss%')
  - `product_delivery_type` = 'Self-Managed'
  - `product_rate_plan_name` NOT IN ('Premium - 1 Year - Eval')
  - `charge_type` = 'Recurring'

**Other Comments:**
- Sums, Counts and Percents of Usage (called metrics) is captured along wth the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not. 
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).  
- Multiple Instances can be hosted on each Implementation. Multiple Installations can be included within each Instance which is determined by Host_id.   (Instance_id || Host_id = Installation_id)  
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinant to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.  
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs mart_ping_instance %}

**Description:** Atomic Level Service Ping information with Subscription, Account and Product information by  Instance, Host and Date.  Metrics are not included in this data. 

**Data Grain:**
- dim_instance_id
- dim_host_id
- dim_ping_date_id

**Filters:**
- none -

**Business Logic in this Model:** 
- `is_last_ping_of_month` = last ping (Instance_id and Host_id) sent for the Month
- `ping_delivery_type` = 'SaaS' WHERE UUID/Instance_id = ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f ELSE 'Self-Managed'
- `is_internal` = TRUE WHERE:
  - UUID/Instance_id = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f' 
  - (OR) installation_type = 'gitlab-development-kit'
  - (OR) hostname = 'gitlab.com' 
  - (OR) hostname LIKE '%.gitlab.com'
- `is_staging` = TRUE WHERE:
  - hostname LIKE 'staging.%'
  - (OR) hostname IN ('staging.gitlab.com','dr.gitlab.com')
- `major_minor_version` = major_version || '.' || minor_version 
- `major_minor_version_id` = major_version * 100 + minor_version
- `version_is_prerelease` = version LIKE '%-pre'
- `cleaned_edition` = 'EE Free' WHERE license_expires_at < ping_created_at ELSE ping_edition 
- `is_program_subscription` = TRUE WHERE product_rate_plan_name LIKE ('%edu%' or '%oss%')
- `arr` = mrr * 12

**Other Comments:**
- The `fct_ping_instance` table is built directly from the [prep_ping_instance table](https://gitlab-data.gitlab.io/analytics/#!/model/model.gitlab_snowflake.prep_ping_instance) which brings in Instance Service Ping data one record per Service Ping.  The Payload column is not utilized in model and therefore there are not metrics in this data. 
- Sums, Counts and Percents of Usage (called metrics) is captured along wth the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not. 
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).  
- Multiple Instances can be hosted on each Implementation. Multiple Installations can be included within each Instance which is determined by Host_id.   (Instance_id || Host_id = Installation_id)  
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinant to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.  
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs mart_ping_instance_metric_monthly %}

**Description:** Atomic Level and Enriched Service Ping Metric information by Instance, Host, Metric, Month  
- Extra business related attributes for License, Subscription and Billing are included to allow single table queries more easily.

**Data Grain:**
- dim_instance_id
- dim_host_id
- metrics_path
- ping_created_at

**Filters:**
- Includes metrics for 28 Day and All-Time timeframes
- Include only the `Last Pings of the Month`
- Metric_value >= 0

**Business Logic in this Model:** 
- `is_last_ping_of_month` = last ping (Instance_id and Host_id) sent for the Month
- `ping_delivery_type` = 'SaaS' WHERE UUID/Instance_id = ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f ELSE 'Self-Managed'
- `is_internal` = TRUE WHERE:
  - UUID/Instance_id = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f' 
  - (OR) installation_type = 'gitlab-development-kit'
  - (OR) hostname = 'gitlab.com' 
  - (OR) hostname LIKE '%.gitlab.com'
- `is_staging` = TRUE WHERE:
  - hostname LIKE 'staging.%'
  - (OR) hostname IN ('staging.gitlab.com','dr.gitlab.com')
- `major_minor_version` = major_version || '.' || minor_version 
- `major_minor_version_id` = major_version * 100 + minor_version
- `version_is_prerelease` = version LIKE '%-pre'
- `dim_ping_instance_id` = 'source ping id' with the following grain:     
  - dim_instance_id
  - dim_host_id
  - ping_created_at
- `is_program_subscription` = TRUE WHERE product_rate_plan_name LIKE ('%edu%' or '%oss%')
- `arr` = mrr * 12
- `latest active subscription` WHERE subscription_status IN (`Active`,`Cancelled`)

**Other Comments:**
- Sums, Counts and Percents of Usage (called metrics) is captured along wth the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not. 
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).  
- Multiple Instances can be hosted on each Implementation. Multiple Installations can be included within each Instance which is determined by Host_id.   (Instance_id || Host_id = Installation_id)  
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinant to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.  
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}
