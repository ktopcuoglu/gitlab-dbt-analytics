{% docs mart_usage_ping_counters_statistics %}

Data mart to explore statistics around usage ping counters. This includes the following statistics:

* first version
* first major version
* first minor version
* last version
* last major version
* last minor version

{% enddocs %}

{% docs mart_paid_subscriptions_monthly_usage_ping_optin %}

Data Mart to explore Usage Ping Opt In rate of Paid subscriptions. The report looks at monthly active subscriptions, along with their licenses and linked usage ping payloads received in order to retrieve an opt-in rate and the application version used by a specific subscription.

This SQL query will pull opt-in rate per month for Paid subscriptions:

```sql

SELECT
    mrr_month,
    AVERAGE(has_sent_payloads::BOOLEAN)
FROM analytics.mart_paid_subscriptions_monthly_usage_ping_optin
GROUP BY 1

```

Here is an image documenting the ERD for this table:

{% enddocs %}
