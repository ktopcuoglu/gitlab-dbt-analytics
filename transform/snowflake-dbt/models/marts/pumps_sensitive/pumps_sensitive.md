{% docs pump_marketing_contact %}

a copy of mart_marketing_contact for sending to Marketo for use in email campaigns

{% enddocs %}

{% docs pump_subscription_product_usage %}

A copy of `subscription_product_usage_data` model for sending to Salesforce

{% enddocs %}

{% docs pump_marketing_premium_to_ultimate %}

A subset of pump_marketing_contact, with all of the same columns, that supports the premium to ultimate email marketing campaign and is the proof of concept pump for the email marketing data pump.

{% enddocs %}

{% docs pump_product_usage_free_user_metrics_monthly %}
This table pulls in data from `mart_product_usage_free_users_metrics_monthly` which contains the sets of all Self-Managed and SaaS **free users**. The data from this table  pumped to Salesforce will be used for Customer Product Insights.

The grain of this table is namespace || uuid-hostname per month.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}
