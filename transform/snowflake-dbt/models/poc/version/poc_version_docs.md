{% docs monthly_usage_data_28_days %}



{% enddocs %}

{% docs monthly_usage_data_all_time %}

This model transforms all_time counters stored in the usage data payload into monthly proxies. To do so the logic is followed is:

* For one specific instance (instance_uid), one specific metric (metrics_name) and one specific month M, take the latest received data
* Compare this data point with the one received one month before, we have 3 scenarii:
  * data from month M > data from month M-1: the monthly proxied metric is data(M) - data(M-1)
  * no data for month M-1: then monthly proxied metric is data(M)
  * data from month M < data from month M-1: abnormal behavior which might be due to an instance reset, then proxied metric is 0

{% enddocs %}

{% docs monthly_usage_data %}

Union of models `monthly_usage_data_28_days` and `monthly_usage_data_all_time`

{% enddocs %}


{% docs usage_data_28_days_flattened %}

The granularity of this model is one row per tuple (metric_name, instance_id).

Usage ping's data is stored in several nested jsons as shown in this page. 

Those metrics sent could be of various types:
* all_time counters (for example how many issues a specific instance has created since its inception)
* 28_days counters (how many users have created at least one issue over the last 4 months)
* an instance configuration parameter (has this instance enabled saml/sso)

This model extracts the 28-days counters (based on the mapping table in this spreadsheet) and flattens the json. 

The models transforms this json:

into this table:


{% enddocs %}

{% docs usage_data_all_time_flattened %}



{% enddocs %}
