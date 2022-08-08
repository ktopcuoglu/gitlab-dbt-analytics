{% docs mart_event_valid %}

**Description:** Enriched GitLab.com Usage Event Data for Valid Events
- [Targets and Actions](https://docs.gitlab.com/ee/api/events.html) activity by Users and [Namespaces](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/namespace/) within the GitLab.com application are captured and refreshed periodically throughout the day.  Targets are objects ie. issue, milestone, merge_request and Actions have effect on Targets, ie. approved, closed, commented, created, etc.
- The data is Enriched with Extra business related attributes for Namespace, User and Project to allow single table queries that satisfy a Larger Generalized set of Use Cases.

**Data Grain:**
- event_id
- event_created_at

**Filters:**
- Use Valid Events Only for standard analysis and reporting:
  - Remove Events where the Event Created Datetime < the User Created Datetime.
    - These are usually events from projects that were created before the User and then imported in by the User after the User is created.  
  - Keep Events where User Id = NULL.  These do not point to a particular User, ie. 'milestones'
  - Remove Events from blocked users
- Rolling 24mos of Data  

**Business Logic in this Model:**
- Valid events where the Event Create DateTime is >= User Create DateTime
- Events from blocked users are excluded
- Event, User and Ultimate_Namespace counts are included for the Aggregation Level

**Other Comments:**
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}

{% docs mart_event_namespace_daily %}

**Description:** Enriched GitLab.com Usage Event Data Grouped by Date, Event, Namespace and Billing for Valid Events with extra Namespace Attributes
- [Targets and Actions](https://docs.gitlab.com/ee/api/events.html) activity by Users and [Namespaces](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/namespace/) within the GitLab.com application are captured and refreshed periodically throughout the day.  Targets are objects ie. issue, milestone, merge_request and Actions have effect on Targets, ie. approved, closed, commented, created, etc.  
- The data is aggregated by Date, Event and Namespace and includes supporting Attributes with Enhanced Namespace Attributes.

**Data Grain:**
- event_date
- event_name
- dim_ultimate_parent_namespace_id

**Filters:**
- Use Valid Events Only for standard analysis and reporting:
  - Remove Events where the Event Created Datetime < the User Created Datetime.
    - These are usually events from projects that were created before the User and then imported in by the User after the User is created.  
  - Keep Events where User Id = NULL.  These do not point to a particular User, ie. 'milestones'
  - Remove Events from blocked users
- Rolling 24mos of Data  

**Business Logic in this Model:**
- Valid events where the Event Create DateTime is >= User Create DateTime
- Events from blocked users are excluded
- The Ultimate Parent Namespace, Plan, Subscription, Billing and Product Information for the Event is determined by the Event Date.
- Each Event is identified as being used for different xMAU metrics (is_smau, is_gmau, is_umau)
- `data_source` = 'GITLAB_DOTCOM'

**Other Comments:**
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}

{% docs mart_event_user_daily %}
Enhanced version of `common.fct_event_user_daily`

**Description:** Enriched GitLab.com Usage Event Data with Only Valid Events by Event_Date, User, Ultimate_Parent_Namespace and Event_Name with extra Namespace Attributes
- [Targets and Actions](https://docs.gitlab.com/ee/api/events.html) activity by Users and [Namespaces](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/namespace/) within the GitLab.com application are captured and refreshed periodically throughout the day.  Targets are objects ie. issue, milestone, merge_request and Actions have effect on Targets, ie. approved, closed, commented, created, etc.
- The data is aggregated by Event_Date, User, Ultimate_Parent_Namespace and Event_Name and includes supporting Attributes with Enhanced Namespace Attributes.

**Data Grain:**
- event_date
- dim_user_id
- dim_ultimate_parent_namespace_id
- event_name

**Filters:**
- Use ONLY Valid Events for standard analysis and reporting:
  - Remove Events where the Event Created Datetime < the User Created Datetime.
    - These are usually events from projects that were created before the User and then imported in by the User after the User is created.  
  - Keep Events where User Id = NULL.  These do not point to a particular User, ie. 'milestones'
  - Remove Events from blocked users
- Rolling 24mos of Data  

**Business Logic in this Model:**
- Valid events where the Event Create DateTime is >= User Create DateTime
- Events from blocked users are excluded

**Other Comments:**
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}

{% docs mart_ping_instance_metric %}

**Description:** Atomic Level Instance Service Ping data by Ping and Metric
- Extra attributes for License, Subscription and Billing are included to allow single table queries more easily.

**Data Grain:**
- dim_ping_instance_id
- metrics_path

**Filters:**
- For SaaS, include only production gitlab.com installation

**Business Logic in this Model:**
- `is_last_ping_of_month` = last ping created per calendar month per Installation (`dim_installation_id`)
- Metrics that timed out (return -1) are set to a value of 0
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
- Sums, Counts and Percents of Usage (called metrics) is captured along with the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not.
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).  
- Multiple Instances can be hosted on each Implementation. Multiple Installations can be included within each Instance which is determined by Host_id.   (Instance_id || Host_id = Installation_id)  
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.  
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs mart_ping_instance_metric_7_day %}

**Description:** Atomic Level Instance Service Ping data by Ping and Metric
- Extra attributes for License, Subscription and Billing are included to allow single table queries more easily.

**Data Grain:**
- dim_ping_instance_id
- metrics_path

**Filters:**
- For SaaS, include only production gitlab.com installation
- Filter to time_frame = 7 day to pull in only 7 day metrics

**Business Logic in this Model:**
- `is_last_ping_of_month` = last ping created per calendar month per Installation (`dim_installation_id`)
- Metrics that timed out (return -1) are set to a value of 0
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
- Sums, Counts and Percents of Usage (called metrics) is captured along with the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not.
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).  
- Multiple Instances can be hosted on each Implementation. Multiple Installations can be included within each Instance which is determined by Host_id.   (Instance_id || Host_id = Installation_id)  
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.  
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs mart_ping_instance_metric_28_day %}

**Description:** Atomic Level Instance Service Ping data by Ping and Metric
- Extra attributes for License, Subscription and Billing are included to allow single table queries more easily.

**Data Grain:**
- dim_ping_instance_id
- metrics_path

**Filters:**
- For SaaS, include only production gitlab.com installation
- Filter to time_frame = 28 day to pull in only 28 day metrics

**Business Logic in this Model:**
- `is_last_ping_of_month` = last ping created per calendar month per Installation (`dim_installation_id`)
- Metrics that timed out (return -1) are set to a value of 0
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
- Sums, Counts and Percents of Usage (called metrics) is captured along with the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not.
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).  
- Multiple Instances can be hosted on each Implementation. Multiple Installations can be included within each Instance which is determined by Host_id.   (Instance_id || Host_id = Installation_id)  
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.  
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs mart_ping_instance_metric_all_time %}

**Description:** Atomic Level Instance Service Ping data by Ping and Metric
- Extra attributes for License, Subscription and Billing are included to allow single table queries more easily.

**Data Grain:**
- dim_ping_instance_id
- metrics_path

**Filters:**
- For SaaS, include only production gitlab.com installation
- Filter to time_frame = all to pull in only all time metrics

**Business Logic in this Model:**
- `is_last_ping_of_month` = last ping created per calendar month per Installation (`dim_installation_id`)
- Metrics that timed out (return -1) are set to a value of 0
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
- Sums, Counts and Percents of Usage (called metrics) is captured along with the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not.
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).  
- Multiple Instances can be hosted on each Implementation. Multiple Installations can be included within each Instance which is determined by Host_id.   (Instance_id || Host_id = Installation_id)  
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.  
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs mart_ping_instance %}

**Description:** Atomic Level Service Ping information with Subscription, Account and Product information by  Ping.  Metrics are not included in this data.

**Data Grain:**
- dim_ping_instance_id

**Filters:**
- For SaaS, include only production gitlab.com installation

**Business Logic in this Model:**
- `is_last_ping_of_month` = last ping created per calendar month per Installation (`dim_installation_id`)
- Metrics that timed out (return -1) are set to a value of 0
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
- Sums, Counts and Percents of Usage (called metrics) is captured along with the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not.
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).  
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_instance_id` is synonymous with `uuid` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- Multiple Instances can be hosted on each Implementation. Multiple Installations can be included within each Instance which is determined by Host_id. (Instance_id || Host_id = Installation_id)
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.  
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs mart_ping_instance_metric_monthly %}

**Description:** Atomic Level and Enriched Service Ping Metric information by Ping and Metric
- Extra business related attributes for License, Subscription and Billing are included to allow single table queries more easily.

**Data Grain:**
- dim_ping_instance_id
- metrics_path

**Filters:**
- Includes metrics for 28 Day and All-Time timeframes
- Include only the `Last Pings of the Month`
- For SaaS, include only production gitlab.com installation

**Business Logic in this Model:**
- `is_last_ping_of_month` = last ping created per calendar month per Installation (`dim_installation_id`)
- Metrics that timed out (return -1) are set to a value of 0
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
- Sums, Counts and Percents of Usage (called metrics) is captured along with the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not.
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).  
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_instance_id` is synonymous with `uuid` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- Multiple Instances can be hosted on each Implementation. Multiple Installations can be included within each Instance which is determined by Host_id. (Instance_id || Host_id = Installation_id)
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.  
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs mart_ping_instance_metric_weekly %}

**Description:** Atomic Level and Enriched Service Ping Metric information by Ping and Metric
- Extra business related attributes for License, Subscription and Billing are included to allow single table queries more easily.

**Data Grain:**
- dim_ping_instance_id
- metrics_path

**Filters:**
- Includes metrics for 7 Day timeframe
- Include only the `Last Pings of the Week`
- For SaaS, include only production gitlab.com installation

**Business Logic in this Model:**
- `is_last_ping_of_week` = last ping created per calendar week per Installation (`dim_installation_id`)
- Metrics that timed out (return -1) are set to a value of 0
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
- Sums, Counts and Percents of Usage (called metrics) is captured along with the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not.
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).  
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_instance_id` is synonymous with `uuid` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- Multiple Instances can be hosted on each Implementation. Multiple Installations can be included within each Instance which is determined by Host_id. (Instance_id || Host_id = Installation_id)
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.  
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}
