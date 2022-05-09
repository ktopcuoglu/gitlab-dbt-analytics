{% docs fct_ping_instance_metric %}

Type of Data: Instance-level Service Ping from Versions app
Aggregate Grain: One record per service ping (dim_ping_instance_id) per metric (metrics_path)
Time Grain: None
Use case: Service Ping metric-level analysis
Notes: Includes non-numeric metric values (ex: instance settings). Metrics that timed out (return -1) are set to a value of 0.

{% enddocs %}

{% docs fct_ping_instance_metric_28_day %}

Type of Data: Instance-level Service Ping from Versions app
Aggregate Grain: One record per service ping (dim_ping_instance_id) per metric (metrics_path)
Time Grain: None
Use case: Service Ping metric-level analysis
Notes: Includes non-numeric metric values (ex: instance settings). Metrics that timed out (return -1) are set to a value of 0. Filtered down to 28 day time_frame.

{% enddocs %}

{% docs fct_ping_instance_metric_7_day %}

Type of Data: Instance-level Service Ping from Versions app
Aggregate Grain: One record per service ping (dim_ping_instance_id) per metric (metrics_path)
Time Grain: None
Use case: Service Ping metric-level analysis
Notes: Includes non-numeric metric values (ex: instance settings). Metrics that timed out (return -1) are set to a value of 0. Filtered down to 7 day time_frame.

{% enddocs %}

{% docs fct_ping_instance_metric_all_time %}

Type of Data: Instance-level Service Ping from Versions app
Aggregate Grain: One record per service ping (dim_ping_instance_id) per metric (metrics_path)
Time Grain: None
Use case: Service Ping metric-level analysis
Notes: Includes non-numeric metric values (ex: instance settings). Metrics that timed out (return -1) are set to a value of 0. Filtered down to all time time_frame.

{% enddocs %}

{% docs mart_ping_instance_metric %}

Type of Data: Instance-level Service Ping from Versions app
Aggregate Grain: One record per service ping (dim_ping_instance_id) per metric (metrics_path)
Time Grain: None
Use case: Service Ping metric exploration and analysis
Note: This model is filtered to metrics that return numeric values. Metrics that timed out are set to a value of 0.

{% enddocs %}

{% docs mart_ping_instance_metric_28_day %}

Type of Data: Instance-level Service Ping from Versions app
Aggregate Grain: One record per service ping (dim_ping_instance_id) per 28-day metric (metrics_path)
Time Grain: None
Use case: Service Ping 28-day metric exploration and analysis
Note: This model is filtered to metrics where time_frame = 28d (https://metrics.gitlab.com/?q=28d). Metrics that timed out are set to a value of 0.

{% enddocs %}

{% docs mart_ping_instance_metric_7_day %}

Type of Data: Instance-level Service Ping from Versions app
Aggregate Grain: One record per service ping (dim_ping_instance_id) per 28-day metric (metrics_path)
Time Grain: None
Use case: Service Ping 7-day metric exploration and analysis
Note: This model is filtered to metrics where time_frame = 7d (https://metrics.gitlab.com/?q=7d). Metrics that timed out are set to a value of 0.

{% enddocs %}

{% docs mart_ping_instance_metric_all_time %}

Type of Data: Instance-level Service Ping from Versions app
Aggregate Grain: One record per service ping (dim_ping_instance_id) per 28-day metric (metrics_path)
Time Grain: None
Use case: Service Ping all-time metric exploration and analysis
Note: This model is filtered to metrics where time_frame = all (https://metrics.gitlab.com/?q=all). Metrics that timed out are set to a value of 0.

{% enddocs %}

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

Data mart to explore statistics around usage ping counters. This includes the following statistics:

  * first version
  * first major version
  * first minor version
  * last version
  * last major version
  * last minor version

{% enddocs %}

{% docs rpt_ping_instance_subcription_opt_in_monthly %}

Monthly counts of active subscriptions.

{% enddocs %}

{% docs rpt_ping_instance_subcription_metric_opt_in_monthly %}

Monthly counts of active subscriptions.

{% enddocs %}

{% docs fct_performance_indicator_targets %}

New fact table to replace performance_indicators_yaml_historical. This new table will include all flattened target values for each metric for each month. Can just filter this fact table down in td_xmau 2.0 snippet

{% enddocs %}
