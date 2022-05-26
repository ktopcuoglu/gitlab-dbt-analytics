{% docs mart_ping_instance_metric %}

Below are some details about the mart model:

* Type of Data: `Instance-level Service Ping from Versions app`
* Aggregate Grain: `One record per service ping (dim_ping_instance_id) per metric (metrics_path)`
* Time Grain: `None`
* Use case: `Service Ping metric exploration and analysis`

Note: `This model is filtered to metrics that return numeric values. Metrics that timed out are set to a value of 0.`

{% enddocs %}

{% docs mart_ping_instance %}

Below are some details about the mart model:

* Type of Data: `Instance-level Service Ping from Versions app`
* Aggregate Grain: `One record per service ping (dim_ping_instance_id)`
* Time Grain: `None`
* Use case: `Service Ping metric exploration and analysis`

Note: This model is unflattened service ping data.

{% enddocs %}

{% docs mart_ping_instance_metric_monthly %}

                Below are some details about the mart model:

                * Type of Data: `Instance-level Service Ping from Versions app`
                * Aggregate Grain: `One record per service ping (dim_ping_instance_id) per 28-day metric (metrics_path)`
                * Time Grain: `None`
                * Use case: `Service Ping 7-day metric exploration and analysis`

                Note: This model is filtered to metrics where time_frame is equal to 28d or all. Only last ping of month shows as well. Metrics that timed out are set to a value of 0.

**Description:** Service Ping Instance Level Data by Instance, Metric, Month
- Sums, Counts and Percents of Usage (called metrics) along wth the Server Instance Configuration information is collected at a point in time for each Instance and sent to GitLab Corporate.  This is normally done on a weekly basis.  The Instance Owner determines whether this data will be sent or not and how will be sent if so.   Implementations can be Customer Hosted (Self-Managed) or GitLab Hosted (referred to as SaaS or Dotcom data).  Multiple Instances can be hosted on Self-Managed Implementations. 
- The data is Enriched with Extra business related attributes for License, Subscription and Billing to allow single table queries that satisfy a Larger Generalized set of Use Cases.

**Data Grain:**
- dim_instance_id
- metrics_path
- ping_created_at_month

**Filters:**
- Includes metrics for 28 Day and ALL timeframes
- Include only the `Last Pings of the Month`
- Include `has_timed_out' = FALSE (to remove Pings that have timed out during processing and may have imcomplete data)
- Include `metric_value' IS NOT NULL - ???
- Include `metric_value` >= 0 (-1 means timed out and -1000 means ???)

**Business Logic in this Model:** 
- `data_source` = 'VERSION_DB'
  - Currently this model only brings in data from Self-Managed Implementations which goes through the Versions Application and Database
- The Ping of the Month - ???     

**Other Comments:**
- The `fct_ping_instance` table is built directly from the [prep_ping_instance table](https://gitlab-data.gitlab.io/analytics/#!/model/model.gitlab_snowflake.prep_ping_instance) which brings in All Service Ping data one record per Service Ping to include a 'Payload' column with all Metrics currently captured in the Service Ping.  There are 2000+ metricsin number. 
- Service Ping data (configuration and metrics) is captured at a particular point in time for a specific grain, ie. Instance.  The metrics within the Service Ping are for different time-frames (ALL, 7 Day and 28 Day).  For this reason Metrics between pings can not be aggregated.  Service Pings are normally compared WoW, MoM, etc.  
- The different types of Service Pings are show here with the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping) and the [GitLab Hosted Implementation](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping)
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}
