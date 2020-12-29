{% docs mart_monthly_counter_adoption %}

Model that allows for a specific unique Usage Ping counter (metrics_path), an edition (CE vs EE) and month, the Estimated Ratio of instances (out of all up-and-running instances) that are sending us a value on a given month.

This ratio will be used to calculate Estimated Values for XMAU or other normal PIs. The estimation Algorithm is [explained in detail here](https://about.gitlab.com/handbook/business-ops/data-team/data-catalog/xmau-analysis/#going-from-recorded-to-estimated-xmau) 

{% enddocs %}
