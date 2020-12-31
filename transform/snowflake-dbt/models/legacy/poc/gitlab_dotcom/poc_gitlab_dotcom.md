{% docs gitlab_dotcom_dev_xmau_mart %}

This table is based on [gitlab_dotcom_daily_usage_data_events](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.gitlab_dotcom_daily_usage_data_events) and as a result only includes dot-com data aggregated at the top most namespace by day. Please know that each month is only the last 28 days of each month for consistency (for example Jan 4-31) between self hosted and dot-com as well as consistency of comparing month to month (even amount of days).

Model for our [Dev Section Dashboard POC](https://app.periscopedata.com/app/gitlab/740233/MP:-Dev-Section-WIP). This is an abstraction layer on top of `gitlab_dotcom_daily_usage_data_events` to:
* calculate more complex events as a UNION of several events
* calculate monthly KPIs based on the evenst defined in the `event_ctes` config variable.
* to add some nuance into this model. Add groups columns to quickly calculate GMAU

You can view the [sql code](https://gitlab.com/-/ide/project/gitlab-data/analytics/edit/master/-/transform/snowflake-dbt/models/poc/gitlab_dotcom/gitlab_dotcom_dev_xmau_mart.sql)

{% enddocs %}
