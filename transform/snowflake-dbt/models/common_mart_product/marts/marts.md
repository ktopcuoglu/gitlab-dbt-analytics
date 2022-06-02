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

{% enddocs %}
