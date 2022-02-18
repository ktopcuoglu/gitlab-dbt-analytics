{% docs fct_usage_event %}

Union of gitlab_dotcom and service_ping sources with additional dim id's

{% enddocs %}


{% docs mart_usage_event %}

GitLab.com Event-Level mart - Sourced from fct_event_usage_metrics

This table contains all gitlab.com events with additional dimensions and other facilitating fields.

{% enddocs %}


{% docs mart_usage_namespace_daily %}

GitLab.com Namespace-Level mart - Sourced from fct_event_usage_metrics

This table contains all gitlab.com events with additional dimensions and other facilitating fields and then is aggregated at the namespace level.

{% enddocs %}


{% docs mart_usage_instance_daily %}

GitLab.com Event-Level mart - Sourced from fct_event_usage_metrics

This table contains all gitlab.com events with additional dimensions and other facilitating fields and then is aggregated at the instance level.

{% enddocs %}


{% docs mart_usage_event_daily %}

GitLab.com Event-Level Daily mart - Sourced from fct_event_usage_metrics

This table contains all gitlab.com events with additional dimensions and other facilitating fields aggregated at a date/user/namespace/plan/event grain

{% enddocs %}
