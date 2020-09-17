{% docs anonymized_snowplow_pageviews_30 %}

Model for our [Dev Section Dashboard POC](https://app.periscopedata.com/app/gitlab/740233/MP:-Dev-Section-WIP). This is an abstraction layer on top of `gitlab_dotcom_daily_usage_data_events` to:
* calculate more complex events as a UNION of several events
* calculate monthly KPIs based on the evenst defined in the `event_ctes` config variable.

{% enddocs %}
