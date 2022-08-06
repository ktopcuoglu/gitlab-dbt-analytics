{% docs mart_paid_subscriptions_monthly_usage_ping_optin %}

Data Mart to explore Usage Ping Opt In rate of Paid subscriptions. The report looks at monthly active subscriptions, along with their licenses and linked usage ping payloads received in order to retrieve an opt-in rate and the application version used by a specific subscription.

This SQL query will pull opt-in rate per month for Paid subscriptions:

```sql

SELECT
    mrr_month,
    AVERAGE(has_sent_payloads::BOOLEAN)
FROM legacy.mart_paid_subscriptions_monthly_usage_ping_optin
GROUP BY 1

```

Here is an image documenting the ERD for this table:

{% enddocs %}

{% docs mart_usage_ping %}

Data mart to explore usage pings. This model excludes all pings received from 'staging.gitlab.com', as well as any ping received without an instance identifier (uuid).

Two notable filtering criteria are included in this model: `usage_ping_delivery_type`, and `is_last_ping_in_month`. `usage_ping_delivery_type` can be used to separate pings from the Gitlab.com instance (`SaaS`) from individual self-managed instances (`Self-Managed`). `is_last_ping_in_month` can be used to filter data down to the last ping received from each instance in a given month for month-to-month comparisons.

As part of the usage ping payload, pings from customers with paid licenses will return a `license_md5`, as well as summary license information. Customer information (such as `dim_billing_account_id`, `dim_crm_account_id`, etc) is available in this model when the returned `license_md5` corresponds to a valid record in the license app. `ping_product_tier` and `ping_license_is_trial` are derived from the usage ping payload, and included in this model for pings where customer information cannot be found from `license_md5`.

{% enddocs %}
