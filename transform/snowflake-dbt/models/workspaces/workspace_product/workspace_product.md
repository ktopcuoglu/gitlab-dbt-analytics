{% docs fct_event_usage_metrics %}

Union of gitlab_dotcom and service_ping sources with additional dim id's

{% enddocs %}


{% docs mart_event_metric_namespace %}

Union of gitlab_dotcom and service_ping event counts at a common grain, grouped by namespace_id with additional dim ID's to tie to DIM tables.

Analysts can quickly filter to gitlab_dotcom events by setting source =  'GITLAB_DOTCOM'.

Analysts can quickly filter to usage_ping events by setting source =  'SERVICE PINGS'.

{% enddocs %}
