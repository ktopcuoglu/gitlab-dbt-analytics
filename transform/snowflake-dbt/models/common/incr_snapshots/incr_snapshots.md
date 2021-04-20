{% docs fct_mrr_snapshot_model %}

A dimension table for marketing contacts, combining contacts from GitLab.com, Salesforce, CustomerDB, and Zuora Sources.

{% enddocs %}

{% docs dim_subscription_snapshot_model %}

This table aggregates data from namespaces, orders, and subscriptions to the level of a marketing contact. Therefore a marketing contact can fall into multiple plan types. An individual could be a Free Individual namespace owner, a member of an Ultimate Group Namespace, and an Owner of a Premium Group Namespace. Each column aggregates data to answers a specific question at the contact level.

{% enddocs %}
