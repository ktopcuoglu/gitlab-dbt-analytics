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

{% docs rpt_ping_instance_subscription_opt_in_monthly %}

Monthly counts of active subscriptions.

{% enddocs %}

{% docs rpt_ping_instance_subscription_metric_opt_in_monthly %}

Monthly counts of active subscriptions.

{% enddocs %}
