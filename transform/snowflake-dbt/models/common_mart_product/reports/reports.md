{% docs rpt_event_xmau_metric_monthly %}

**Description:** GitLab.com Usage Event Report Data with Monthly Totals for Valid Free and Paid Events for User Type Events that are Used in xMAU Metrics
- [Targets and Actions](https://docs.gitlab.com/ee/api/events.html) activity by Users and [Namespaces](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/namespace/) within the GitLab.com application are captured and refreshed periodically throughout the day.  Targets are objects ie. issue, milestone, merge_request and Actions have effect on Targets, ie. approved, closed, commented, created, etc.  

**Data Grain:**
- event_calendar_month
- user_group
- section_name
- stage_name
- group_name (plan_was_paid_at_event_date)

**Filters:**
- Use Valid Events Only for standard analysis and reporting:
  - Remove Events where the Event Created Datetime < the User Created Datetime.
    - These are usually events from projects that were created before the User and then imported in by the User after the User is created.  
  - Keep Events where User Id = NULL.  These do not point to a particular User, ie. 'milestones' 
  - Remove Events from blocked users
- Rolling 24mos of Data
- Include rows where the Event_Date is within 28 days of the Last Day of the Month
- Include User Type Events 
- Include Events used in Metrics (umau, gmau, smau)  

**Business Logic in this Model:** 
- Valid events where the Event Create DateTime is >= User Create DateTime
- Events from blocked users are excluded
- Aggregated Counts are based on the Event Date being within the Last Day of the Month and 27 days prior to the Last Day of the Month (total 28 days)
  - Events that are 29,30 or 31 days prior to the Last Day of the Month will Not be included in these totals
  - This is intended to match the instance-level service ping metrics by getting a 28-day count
- The Last Plan Id of the Month for the Namespace is used for the Calculations and Reporting.

**Other Comments:**
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}

{% docs rpt_event_plan_monthly %}

**Description:** GitLab.com Usage Event Report Data with Monthly Totals for Valid Events
- [Targets and Actions](https://docs.gitlab.com/ee/api/events.html) activity by Users and [Namespaces](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/namespace/) within the GitLab.com application are captured and refreshed periodically throughout the day.  Targets are objects ie. issue, milestone, merge_request and Actions have effect on Targets, ie. approved, closed, commented, created, etc.  

**Data Grain:**
- event_calendar_month
- event_name
- user_group (plan_was_paid_at_event_date)

**Filters:**
- Use Valid Events Only for standard analysis and reporting:
  - Remove Events where the Event Created Datetime < the User Created Datetime.
    - These are usually events from projects that were created before the User and then imported in by the User after the User is created.  
  - Keep Events where User Id = NULL.  These do not point to a particular User, ie. 'milestones' 
  - Remove Events from blocked users
- Rolling 24mos of Data
- Include rows where the Event_Date is within 28 days of the Last Day of the Month  

**Business Logic in this Model:** 
- Valid events where the Event Create DateTime is >= User Create DateTime
- Events from blocked users are excluded
- Aggregated Counts are based on the Event Date being within the Last Day of the Month and 27 days prior to the Last Day of the Month (total 28 days)
  - Events that are 29,30 or 31 days prior to the Last Day of the Month will Not be included in these totals
  - This is intended to match the instance-level service ping metrics by getting a 28-day count

**Other Comments:**
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}


{% docs rpt_ping_metric_first_last_versions %}

**Description:**  First and Last Versions for Ping Metrics by Edition and Prerelease
- This table provides First and Last Application Versions along with Installation Counts by Metric, Ping Edition and Prerelease.    

**Data Grain:**
- metrics_path
- ping_edition
- version_is_prerelease

**Filters:**
- Metrics from GitLab Service Pings will not be considered
- `Forwarded` - Only 28 Day and All-Time metrics  
- `Forwarded` - Only Metrics from the 'Last Ping of the Month' pings 

**Business Logic in this Model:** 
- `First Versions` - The earliest version found for each Metrics_Path, Ping_Edition and Version_Is_Prerelease 
- `Last Versions` - The latest version found for each Metrics_Path, Ping_Edition and Version_Is_Prerelease 
- `is_last_ping_of_month` = last ping (Instance_id and Host_id) sent for the Month
- `major_minor_version` = major_version || '.' || minor_version 
- `major_minor_version_id` = major_version * 100 + minor_version
- `version_is_prerelease` = version LIKE '%-pre'

**Other Comments:**
- Service Ping data is Sums, Counts and Percents of Usage (called metrics) along with the Server Instance Configuration information is collected at a point in time for each Instance and sent to GitLab Corporate.  This is normally done on a weekly basis.  The Instance Owner determines whether this data will be sent or not and how much will be sent.  Implementations can be Customer Hosted (Self-Managed) or GitLab Hosted (referred to as SaaS or Dotcom data).  Multiple Instances can be hosted on Self-Managed Implementations like GitLab Implementations. 
- The different types of Service Pings are shown here for the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping) and the [GitLab Hosted Implementation Service Pings](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs rpt_ping_latest_subscriptions_monthly %}

**Description:**  Self-Managed Service Pings with Latest Subscriptions, ARR Charges and Ping Counts by Installation, Month
- Latest Subscription, Version, ARR, MRR and Ping Count information in included. 

**Data Grain:**
- ping_created_at_month
- dim_installation_id

**Filters:**
- Include `ping_delivery_type = 'Self-Managed'`

**Business Logic in this Model:**
- MRR, ARR and Licensed_User_Count is limited to:
  - product_delivery_type = `Self-Managed` 
  - subscription_status IN (`Active`,`Cancelled`)
  - product_tier_name <> `Storage`

**Other Comments:**
- Service Ping data is Sums, Counts and Percents of Usage (called metrics) along with the Server Instance Configuration information is collected at a point in time for each Instance and sent to GitLab Corporate.  This is normally done on a weekly basis.  The Instance Owner determines whether this data will be sent or not and how much will be sent.  Implementations can be Customer Hosted (Self-Managed) or GitLab Hosted (referred to as SaaS or Dotcom data).  Multiple Instances can be hosted on Self-Managed Implementations like GitLab Implementations. 
- The different types of Service Pings are shown here for the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping) and the [GitLab Hosted Implementation Service Pings](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs rpt_ping_subscriptions_on_versions_estimate_factors_monthly %}

**Description:**  Estimated Usage Percents for Subscriptions on a Version by Metric and Month  
- These estimations are used for determining usage for Implementations that do not send Service Pings. 
- Multiple Estimation methods are in this data and utilized by a Macro.   

**Data Grain:**
- ping_created_at_month
- metrics_path
- ping_edition
- estimation_grain

**Filters:**
- Includes metrics for 28 Day timeframe
- Include metrics from pings with `ping_delivery_type = 'Self-Managed'`
- `Forwarded` - Include Metrics on Valid versions
- `Forwarded` - Metrics from GitLab Service Pings will not be considered
- `Forwarded` - Include Metrics from the 'Last Ping of the Month' pings

**Business Logic in this Model:**
- There are multiple estimation Grains in this model
  - A macro is used to determine which `estimation_grain` to use from this report.  
- `estimation_grain` - tells which method is used to measure the `percent_reporting` %:
  - 'reported metric - seat based estimation' is from licensed_users seat counts
  - 'reported metric - subscription based estimation' is derived from subscription counts
- `percent_reporting` - reporting_count / (reporting_count + not_reporting_count) 
  - 'reporting_count' of Service Ping History (active Users or active Subscriptions)
  - 'not_reporting_count' not reporting for the Month (active Users or active Subscriptions)
- `Subscriptions on Valid Versions Estimate Percent Calculation`:
  - (Version Subscriptions) / (All Subscriptions Reporting + All Subscriptions Not Reporting) 
  - (Version Users) / (All Users Reporting + All Users Not Reporting)  
- MRR, ARR and Licensed_User_Count is limited to:
  - product_delivery_type = `Self-Managed` 
  - subscription_status IN (`Active`,`Cancelled`)
  - product_tier_name <> `Storage`

**Other Comments:**
- Service Ping data is Sums, Counts and Percents of Usage (called metrics) along with the Server Instance Configuration information is collected at a point in time for each Instance and sent to GitLab Corporate.  This is normally done on a weekly basis.  The Instance Owner determines whether this data will be sent or not and how much will be sent.  Implementations can be Customer Hosted (Self-Managed) or GitLab Hosted (referred to as SaaS or Dotcom data).  Multiple Instances can be hosted on Self-Managed Implementations like GitLab Implementations. 
- The different types of Service Pings are shown here for the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping) and the [GitLab Hosted Implementation Service Pings](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs rpt_ping_metric_estimate_factors_monthly %}

**Description:**  Estimated Usage Percents for All Reported Subscriptions and Subscriptions on Versions by Metric and Month  
- These estimations are used for determining usage for Implementations that do not send Service Pings. 
- Multiple Estimation methods are in this data and utilized by a Macro.   

**Data Grain:**
- ping_created_at_month
- metrics_path
- ping_edition
- estimation_grain

**Filters for Subscriptions on a Version:**
- Includes metrics for 28 Day timeframe
- Include metrics from pings with `ping_delivery_type = 'Self-Managed'`
- `Forwarded` - Include Metrics on Valid versions
- `Forwarded` - Metrics from GitLab Service Pings will not be considered
- `Forwarded` - Include Metrics from the 'Last Ping of the Month' pings

**Filters for All Reported Subscriptions:**
- `Forwarded` - Include `ping_delivery_type = 'Self-Managed'`
- `Forwarded` - Include metrics from pings with `ping_delivery_type = 'Self-Managed'`
- `Forwarded` - Includes metrics for 28 Day and All-Time timeframes
- `Forwarded` - Include only the `Last Pings of the Month`

**Business Logic in this Model:**
- There are multiple estimation Grains in this model
  - A macro is used to determine which `estimation_grain` to use from this report.  
- `estimation_grain` - tells which method is used to measure the `percent_reporting` %:
  - 'reported metric - seat based estimation' is from licensed_users seat counts
  - 'reported metric - subscription based estimation' is derived from subscription counts
- `percent_reporting` - reporting_count / (reporting_count + not_reporting_count) 
  - 'reporting_count' of Service Ping History (active Users or active Subscriptions)
  - 'not_reporting_count' not reporting for the Month (active Users or active Subscriptions)
- `Subscriptions on Valid Versions Estimate Percent Calculation`:
  - (Version Subscriptions) / (All Subscriptions Reporting + All Subscriptions Not Reporting) 
  - (Version Users) / (All Users Reporting + All Users Not Reporting)  
- `Subscriptions (All) Estimate Percent Calculation`:
  - (All Subscriptions) / (All Subscriptions Reporting + All Subscriptions Not Reporting) 
  - (All Users) / (All Users Reporting + All Users Not Reporting) 
- MRR, ARR and Licensed_User_Count is limited to:
  - product_delivery_type = `Self-Managed` 
  - subscription_status IN (`Active`,`Cancelled`)
  - product_tier_name <> `Storage`

**Other Comments:**
- Service Ping data is Sums, Counts and Percents of Usage (called metrics) along with the Server Instance Configuration information is collected at a point in time for each Instance and sent to GitLab Corporate.  This is normally done on a weekly basis.  The Instance Owner determines whether this data will be sent or not and how much will be sent.  Implementations can be Customer Hosted (Self-Managed) or GitLab Hosted (referred to as SaaS or Dotcom data).  Multiple Instances can be hosted on Self-Managed Implementations like GitLab Implementations. 
- The different types of Service Pings are shown here for the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping) and the [GitLab Hosted Implementation Service Pings](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs rpt_ping_subscriptions_reported_estimate_factors_monthly %}

**Description:**  Estimated Usage Percents for All Subscriptions by Metric and Month  
- These estimations are used for determining usage for Implementations that do not sent Service Pings. 
- Multiple Estimation methods are in this data and utilized by a Macro.   

**Data Grain:**
- ping_created_at_month
- metrics_path
- ping_edition
- estimation_grain

**Filters:**
- `Forwarded` - Include `ping_delivery_type = 'Self-Managed'`
- `Forwarded` - Include metrics from pings with `ping_delivery_type = 'Self-Managed'`
- `Forwarded` - Includes metrics for 28 Day and All-Time timeframes
- `Forwarded` - Include only the `Last Pings of the Month`

**Business Logic in this Model:**
- There are multiple estimation Grains in this model
  - A macro is used to determine which `estimation_grain` to use for this data.  
- `estimation_grain` - tells which method is used to measure the `percent_reporting` %:
  - 'reported metric - seat based estimation' is from licensed_users seat counts
  - 'reported metric - subscription based estimation' is derived from subscription counts
- `percent_reporting` - reporting_count / (reporting_count + not_reporting_count) 
  - 'reporting_count' of Service Ping History (active Users or active Subscriptions)
  - 'not_reporting_count' not reporting for the Month (active Users or active Subscriptions)
- `Subscriptions (All) Estimate Percent Calculation`:
  - (All Subscriptions) / (All Subscriptions Reporting + All Subscriptions Not Reporting) 
  - (All Users) / (All Users Reporting + All Users Not Reporting) 
- MRR, ARR and Licensed_User_Count is limited to:
  - product_delivery_type = `Self-Managed` 
  - subscription_status IN (`Active`,`Cancelled`)
  - product_tier_name <> `Storage`

**Other Comments:**
- Service Ping data is Sums, Counts and Percents of Usage (called metrics) along with the Server Instance Configuration information is collected at a point in time for each Instance and sent to GitLab Corporate.  This is normally done on a weekly basis.  The Instance Owner determines whether this data will be sent or not and how much will be sent.  Implementations can be Customer Hosted (Self-Managed) or GitLab Hosted (referred to as SaaS or Dotcom data).  Multiple Instances can be hosted on Self-Managed Implementations like GitLab Implementations. 
- The different types of Service Pings are shown here for the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping) and the [GitLab Hosted Implementation Service Pings](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs rpt_ping_metric_totals_w_estimates_monthly %}

**Description:**  Usage totals and estimations for Reported and Non-Reported Instances by Month, Metric, Edition, Estimate Grain, Product Tier and Delivery Type     

**Data Grain:**
- ping_created_at_month
- metrics_path
- ping_edition
- estimation_grain
- ping_edition_product_tier
- ping_delivery_type

**Filters:**
- Include metrics for 28 Day timeframes
- `Forwarded` - Include Metrics from the 'Last Ping of the Month' pings

**Business Logic in this Model:**
- `ping_delivery_type` = 'SaaS' WHERE UUID/Instance_id = ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f ELSE 'Self-Managed'
- `is_last_ping_of_month` = last ping (Instance_id and Host_id) sent for the Month
- There are multiple estimation Grains in this model
  - A macro is used to determine which `estimation_grain` to use for this report.  
- `estimation_grain` - tells which method is used to measure the `percent_reporting` %:
  - 'reported metric - seat based estimation' is from licensed_users seat counts
  - 'reported metric - subscription based estimation' is derived from subscription counts
  - 'SaaS' is included for SaaS usage data
- `percent_reporting` - reporting_count / (reporting_count + not_reporting_count) 
  - 'reporting_count' of Service Ping History (active Users or active Subscriptions)
  - 'not_reporting_count' not reporting for the Month (active Users or active Subscriptions)
- `Subscriptions on Valid Versions Estimate Percent Calculation`:
  - (Version Subscriptions) / (All Subscriptions Reporting + All Subscriptions Not Reporting) 
  - (Version Users) / (All Users Reporting + All Users Not Reporting)  
- `Subscriptions (All) Estimate Percent Calculation`:
  - (All Subscriptions) / (All Subscriptions Reporting + All Subscriptions Not Reporting) 
  - (All Users) / (All Users Reporting + All Users Not Reporting) 
- `Estimation Description`:  (There are different methods for Measuring Usage and Estimated Usage, ie. by Subscriptions Counts or User Counts.  The method used will be shown in the `estimation_grain`.)
  - Reporting_count -  Count of Subscriptions or Users that are Reporting for a Metric
  - Not_reporting_count - Count of Subscriptions or Users Not Reporting for a Metric
  - Percent_Reporting - Percent of Subscriptons or Users Reporting from Total Subscriptions or Users
  - Total_usage_with_estimate - (Recorded_usage + (Recorded_usage * (1-Percent_reporting))) / Percent_reporting
  - Estimated_usage - Total_usage_with_estimates - Recorded_usage
  - Recorded_Usage - Actual usage value for the Metric

**Other Comments:**
- Service Ping data is Sums, Counts and Percents of Usage (called metrics) along with the Server Instance Configuration information is collected at a point in time for each Instance and sent to GitLab Corporate.  This is normally done on a weekly basis.  The Instance Owner determines whether this data will be sent or not and how much will be sent.  Implementations can be Customer Hosted (Self-Managed) or GitLab Hosted (referred to as SaaS or Dotcom data).  Multiple Instances can be hosted on Self-Managed Implementations like GitLab Implementations. 
- The different types of Service Pings are shown here for the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping) and the [GitLab Hosted Implementation Service Pings](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs rpt_ping_subscriptions_reported_counts_monthly %}

**Description:**  Ping Metrics by Edition and Month with Subscription, ARR and User Totals by Installation and Month  
- Latest Subscription, Version, ARR, MRR and Ping Count information in included. 

**Data Grain:**
- ping_created_at_month
- metrics_path
- ping_edition

**Filters:**
- `Forwarded` - Include metrics from pings with `ping_delivery_type = 'Self-Managed'`

**Business Logic in this Model:**
- `Forwarded` - ARR and Licensed_User_Count is limited to:
  - product_delivery_type = `Self-Managed` 
  - subscription_status IN (`Active`,`Cancelled`)
  - product_tier_name <> `Storage`

**Other Comments:**
- Service Ping data is Sums, Counts and Percents of Usage (called metrics) along with the Server Instance Configuration information is collected at a point in time for each Instance and sent to GitLab Corporate.  This is normally done on a weekly basis.  The Instance Owner determines whether this data will be sent or not and how much will be sent.  Implementations can be Customer Hosted (Self-Managed) or GitLab Hosted (referred to as SaaS or Dotcom data).  Multiple Instances can be hosted on Self-Managed Implementations like GitLab Implementations. 
- The different types of Service Pings are shown here for the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping) and the [GitLab Hosted Implementation Service Pings](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs rpt_ping_subscriptions_on_versions_counts_monthly %}

**Description:**  Ping Metrics Totals for Subscriptions on Valid Versions by Edition and Month  
- Latest Subscription, Version, ARR, MRR and Ping Count information in included. 

**Data Grain:**
- ping_created_at_month
- metrics_path
- ping_edition

**Filters:**
- Include Metrics on Valid versions
- `Forwarded` - Metrics from GitLab Service Pings will not be considered
- `Forwarded` - Only 28 Day and All-Time metrics  
- `Forwarded` - Only Metrics from the 'Last Ping of the Month' pings
- `Forwarded` - Utilizing 'self_managed' pings only for Metrics listing

**Business Logic in this Model:**
- `Forwarded` - ARR and Licensed_User_Count is limited to:
  - product_delivery_type = `Self-Managed` 
  - subscription_status IN (`Active`,`Cancelled`)
  - product_tier_name <> `Storage`

**Other Comments:**
- Service Ping data is Sums, Counts and Percents of Usage (called metrics) along with the Server Instance Configuration information is collected at a point in time for each Instance and sent to GitLab Corporate.  This is normally done on a weekly basis.  The Instance Owner determines whether this data will be sent or not and how much will be sent.  Implementations can be Customer Hosted (Self-Managed) or GitLab Hosted (referred to as SaaS or Dotcom data).  Multiple Instances can be hosted on Self-Managed Implementations like GitLab Implementations. 
- The different types of Service Pings are shown here for the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping) and the [GitLab Hosted Implementation Service Pings](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}
