{% docs mart_monthly_product_usage %}

This model is at the granularity of a row per counter name, tuple (instance, host) and month. This will allow end users to easily have an answer to this type of questions:

* How many users are running CI Pipelines with Gitlab Products ?
* How many users on a paid instance are interacting with Merge Requests?

A lot of dimensions are currently added to the model such as:

* Subscription Dimensions
  * is_paid_subscription
  * subscription_product_category
  * is_edu_oss
  * is_trial
  * subscription name
  * subscription start date
* Usage Ping Dimensions:
  * host names
  * major_minor_version
  * edition (CE vs EE)
  * delivery (SaaS vs Self-Managed)

Some questions that can be answered:

* Are EDU/OSS subscriptions using the Secure features as much as Paid Subscriptions ?
* Are Subscriptions more mature (longer than 6 months) 
* What are the Top 10 Subscriptions using our Plan Stage ?

This is still a WIP, and we are planning to add some other dimensions such as:

* deal size
* team size
* Industry

{% enddocs %}


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
