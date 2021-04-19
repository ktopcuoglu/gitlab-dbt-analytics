{% docs default_usage_ping_information %}
This macro contains default usage ping information, such as dim_usage_ping_id, license information, or other statistics that help us to identify if the usage ping is valuable. 

This macro is used to support data models that the sales team is requesting these metrics in. 
{% enddocs %}

{% docs sales_wave_2_3_metrics %}
This macro contains the [list of metrics that the Sales CS team requested on FY21-Q1](https://docs.google.com/spreadsheets/d/1ZR7duYmjQ8x86iAJ1dCix88GTtPlOyNwiMgeG_85NiA/edit?ts=5fea3398#gid=0). 

This macro is used to support data models that the sales team is requesting these metrics in. 
{% enddocs %}

{% docs stage_mapping %}
This macro takes in a product stage name, such as 'Verify', and returns a SQL aggregation statement that sums the number of users using that stage, based on the ping data. Product metrics are mapped to stages using the [ping_metrics_to_stage_mapping_data.csv](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/data/ping_metrics_to_stage_mapping_data.csv).
Used in:
{% enddocs %}

{% docs usage_ping_month_range %}
For a given subscription and `_all_time_event` metric, this macro returns two columns containing the first month and last month in which the input metric is included in a Usage Ping payload associated with the subscription.
{% enddocs %}

{% docs usage_ping_over_ping_difference %}
This macro returns two columns for a given subscription in a given month: 
1. The `_all_time_event` metric used as the input for the macro
2. The difference in the input metric count between consecutive Usage Pings
{% enddocs %}

{% docs usage_ping_over_ping_estimated %}
This macro returns three columns for a given subscription in a given month:
1. The `_all_time_event` metric used as the input for the macro
2. The difference in the input metric count between consecutive Usage Pings
3. If the metric is not null in the given month, an estimated monthly metric count, calculated by multiplying the above daily count by the number of days in the given fiscal month, else `NULL`
{% enddocs %}

{% docs usage_ping_over_ping_smoothed %}
This macro returns four columns for a given subscription in a given month:
1. The `_all_time_event` metric used as the input for the macro
2. The difference in the input metric count between consecutive Usage Pings
3. A daily metric count, calculated by dividing the above difference by the number of days between the consecutive Usage Pings used to calculate the above difference
4. An estimated monthly metric count, calculated by multiplying the above daily count by the number of days in the given fiscal month
{% enddocs %}