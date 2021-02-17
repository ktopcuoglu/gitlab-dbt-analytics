{% docs subscription_product_usage_data %}

This model collates a variety of product usage data metrics at the subscription_id granularity for both self-managed and SaaS subscriptions. Detailed documentation on the creation of this model, constraints, and example queries can be found on the [Master Subscription Product Usage Data Process Dashboard](https://app.periscopedata.com/app/gitlab/686439/Master-Subscription-Product-Usage-Data-Process).

{% enddocs %}

{% docs mart_product_usage_wave_1_3_metrics_latest %}

The purpose of this mart table is to act as a data pump of the _most recently received_ **Self-Managed** customer product data into Gainsight for Customer Product Insights.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs mart_product_usage_wave_1_3_metrics_monthly %}

The purpose of this mart table is to act as a data pump of **Self-Managed** customer product data at a _monthly grain_ into Gainsight for Customer Product Insights.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}