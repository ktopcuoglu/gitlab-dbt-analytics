{% docs usage_ping_mart %}

Data mart to explore usage pings. This model excludes all pings received from 'staging.gitlab.com', as well as any ping received without an instance identifier (uuid).

Two notable filtering criteria are included in this model: `ping_source`, and `is_last_ping_in_month`. `ping_source` can be used to separate pings from the Gitlab.com instance (`SaaS`) from individual self-managed instances (`Self-Managed`). `is_last_ping_in_month` can be used to filter data down to the last ping received from each instance in a given month for month-to-month comparisons.

As part of the usage ping payload, pings from customers with paid licenses will return a `license_md5`, as well as summary license information. Customer information (such as `account_id`, `subscription_id`, `product_categories`, etc) is available in this model when the returned `license_md5` corresponds to a valid record in the license app. `ping_product_tier` and `ping_license_is_trial` are derived from the usage ping payload, and included in this model for pings where customer information cannot be found from `license_md5`.

{% enddocs %}