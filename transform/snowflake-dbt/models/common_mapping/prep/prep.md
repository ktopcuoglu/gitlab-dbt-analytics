{% docs prep_sfdc_account %}

SFDC Account Prep table, used to clean and dedupe fields from a common source for use in further downstream dimensions.
Cleaning operations vary across columns, depending on the nature of the source data. See discussion in [MR](https://gitlab.com/gitlab-data/analytics/-/merge_requests/3782) for further details

{% enddocs %}

{% docs prep_campaign %}

Creates a base view with generated keys for the campaign shared dimension and fact and references in facts.

{% enddocs %}

{% docs prep_crm_sales_representative %}

Creates a base view with generated keys for the sales representative and live crm sales hierarchy shared dimensions and references in facts.

{% enddocs %}

{% docs prep_crm_sales_hierarchy_live %}

Creates a base view with generated keys for the live crm sales hierarchy shared dimensions and references in facts.

{% enddocs %}

{% docs prep_crm_sales_hierarchy_stamped %}

Creates a base view with generated keys for the stamped/historical crm sales hierarchy shared dimensions and references in facts. This is built from the stamped fields in the opportunity object and will be used in sales funnel analyses.

{% enddocs %}

{% docs prep_gitlab_dotcom_application_settings_monthly %}

This model captures a historical record of GitLab's default application settings for CI minutes and storage at a monthly grain.

{% enddocs %}

{% docs prep_industry %}

Creates a base view with generated keys for the industry shared dimension and references in facts.

{% enddocs %}

{% docs prep_location_country %}

Creates a base view with generated keys for the geographic country shared dimension and references in facts. It also maps countries to geographic regions.

{% enddocs %}

{% docs prep_location_region %}

Creates a base view with generated keys for the geographic region shared dimension and references in facts.

{% enddocs %}

{% docs prep_bizible_marketing_channel_path %}

Creates a base view with generated keys for the marketing channel path shared dimension and references in facts.

{% enddocs %}

{% docs prep_sales_qualified_source %}

Creates a base view with generated keys for the sales qualified source (source of an opportunity) shared dimension and references in facts.

{% enddocs %}

{% docs prep_order_type %}

Creates a base view with generated keys for the order type shared dimension and references in facts.

{% enddocs %}

{% docs prep_deal_path %}

Creates a base view with generated keys for the deal path shared dimension and references in facts.

{% enddocs %}

{% docs prep_recurring_charge_subscription_monthly %}

Sums MRR and ARR charges by subscription by month. MRR and ARR values are also broken out by delivery type (Self-Managed, SaaS, Others) at the same grain.

To align the subscriptions in this table with `prep_recurring_charge`, filter on `subscription_status IN ('Active', Cancelled')`.

{% enddocs %}

{% docs prep_recurring_charge %}

Creates a base view of charges, including paid and free subscriptions. This base view is used to create fct_mrr by filtering out those free subscriptions.

{% enddocs %}

{% docs prep_sales_segment %}

Creates a base view with generated keys for the sales segment shared dimension and references in facts.

{% enddocs %}

{% docs prep_sales_territory %}

Creates a base view with generated keys for the sales territory shared dimension and references in facts.

{% enddocs %}

{% docs prep_subscription %}

Creates a base view with generated keys for the subscription shared dimension and references in facts.

{% enddocs %}

{% docs prep_product_tier %}

 This table creates keys for the common product tier dimension that is used across gitlab.com and Zuora data sources.

 The granularity of the table is product_tier.

{% enddocs %}

{% docs prep_quote %}

Creates a Quote Prep table for representing Zuora quotes and associated metadata for shared dimension and references in facts.

The grain of the table is quote_id.

{% enddocs %}

{% docs prep_license %}

Creates a License Prep table for representing generated licenses and associated metadata for shared dimension and references in facts.

The grain of the table is license_id.

{% enddocs %}

{% docs prep_usage_ping_subscription_mapped_gmau %}

This data model contains the values of each GMAU metric for **Self-Managed** instances with a non-null `license_md5`. Rows missing a `dim_subscription_id` indicate that either no matching license was found in `map_license_subscription_account`, or no `dim_subscription_id` exists in `map_license_subscription_account` associated with the given `license_md5`.

This data model is used for the Customer Health Dashboards.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs prep_usage_ping_subscription_mapped_smau %}

This data model contains the values of each SMAU metric for **Self-Managed** instances with a non-null `license_md5`. Rows missing a `dim_subscription_id` indicate that either no matching license was found in `map_license_subscription_account`, or no `dim_subscription_id` exists in `map_license_subscription_account` associated with the given `license_md5`.

This data model is used for the Customer Health Dashboards.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs prep_usage_ping_metric_detail %}

This table contains every Usage Ping metric name and path.

{% enddocs %}

{% docs prep_usage_self_managed_seat_link %}

This prep table contains Seat Link data at a daily grain for downstream aggregation and summarization, as well as flags for data quality.

Self-managed EE instances send [Seat Link](https://docs.gitlab.com/ee/subscriptions/self_managed/#seat-link) usage data to [CustomerDot](https://gitlab.com/gitlab-org/customers-gitlab-com) on a daily basis. This information includes a count of active users and a maximum count of users historically in order to assist the [true up process](https://docs.gitlab.com/ee/subscriptions/self_managed/#users-over-license). Additional details can be found in [this doc](https://gitlab.com/gitlab-org/customers-gitlab-com/-/blob/staging/doc/reconciliations.md).

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs prep_subscription_lineage_intermediate %}

The `zuora_subs` CTE de-duplicates Zuora subscriptions. Zuora keeps track of different versions of a subscription via the field "version". However, it's possible for there to be multiple version of a single Zuora version. The data with account_id = '2c92a0fc55a0dc530155c01a026806bd' in the base zuora_subscription table exemplifies this. There are multiple rows with a version of 4. The CTE adds a row number based on the updated_date where a value of 1 means it's the newest version of that version. It also filters subscriptions down to those that have either "Active" or "Cancelled" statuses since those are the only ones that we care about.

The `renewal_subs` CTE creates a lookup table for renewal subscriptions, their parent, and the earliest contract start date. The `contract_effective_date` field was found to be the best identifier for a subscriptions cohort, hence why we're finding the earliest relevant one here. The renewal_row is generated because there are instances where multiple subscriptions point to the same renewal. We generally will want the oldest one for info like cohort date.

The final select statement creates a new field specifically for counting subscriptions and generates appropriate cohort dates. Because we want to count renewal subscriptions as part of their parent, we have the slug for counting so that we don't artificially inflate numbers. It also pickes the most recent version of a subscription.

The subscription_end_month calculation is taken as the previous month for a few reasons. Technically, on Zuora's side, the effective end date stored in the database the day _after_ the subscription ended. (More info here https://community.zuora.com/t5/Subscriptions/How-to-get-ALL-the-products-per-active-subscription/td-p/2224) By subtracting the month, we're guaranteed to get the correct month for an end date. If in the DB it ends 7/31, then in reality that is the day before and is therefore not in effect for the month of July (because it has to be in effect on the last day to be in force for that month). If the end date is 8/1, then it is in effect for the month of July and we're making the proper calculation.

{% enddocs %}

{% docs prep_subscription_lineage %}

Connects a subscription to all of the subscriptions in its lineage. To understand more about a subscription's relationship to others, please see [the handbook under Zuora Subscription Data Management](https://about.gitlab.com/handbook/finance/accounting/)

The `flattening` CTE flattens the intermediate model based on the array in the renewal slug field set in the base subscription model. Lineage is initially set here as the values in the parent slug and any renewal slugs. The OUTER => TRUE setting is like doing an outer join and will return rows even if the renewal slug is null.  

The recursive CTE function generate the full lineage. The anchor query pulls from the flattening CTE and sets up the initial lineage. If there is a renewal subscription then it will continue to the next part of the CTE, but if there are no renewals then the recursive clause will return no additional results.

The recursive clause joins the renewal slug from the anchor clause to the subscription slug of the next iteration of the recursive clause. We're keeping track of the parent slug as the "root" for the initial recursion (this is the "ultimate parent" of the lineage). Within the recursive clause we're checking if there are any further renewals before setting the child count.

The next CTE takes the full union of the results and finds the longest lineage for every parent slug based on the children_count. This CTE is overexpressive and could most likely be simplified with the deduplication CTE. The final dedupe CTE returns a single value for every root and it's full downstream lineage.

{% enddocs %}

{% docs prep_subscription_lineage_parentage_start %}
This is the first part of a two-part model. (It is in two parts because of memory constraints.)

The `flattened` CTE takes the data from lineage, which starts in the following state:


|SUBSCRIPTION_NAME_SLUGIFY|LINEAGE|
|:-:|:-:|
|a-s00011816|a-s00011817,a-s00011818|
|a-s00011817|a-s00011818|
|a-s00003063|a-s00011816,a-s00011817,a-s00011818|


This flattens them to be be in one-per row. Rxample:

|SUBSCRIPTION_NAME_SLUGIFY|SUBSCRIPTIONS_IN_LINEAGE|CHILD_INDEX|
|:-:|:-:|:-:|
|a-s00011817|a-s00011818|0|
|a-s00011816|a-s00011817|0|
|a-s00011816|a-s00011818|1|
|a-s00003063|a-s00011816|0|
|a-s00003063|a-s00011817|1|

Then we identify the version of the `subscriptions_in_lineage` with the max depth (in the `find_max_depth` CTE) and join it to the `flattened` CTE in the `with_parents` CTE. This allows us to identify the ultimate parent subscription in any given subscription.

For this series of subscriptions, the transformation result is:

|ULTIMATE_PARENT_SUB|CHILD_SUB|DEPTH|
|:-:|:-:|:-:|
|a-s00003063|a-s00011816|0|
|a-s00003063|a-s00011817|1|
|a-s00003063|a-s00011818|2|

Of note here is that parent accounts _only_ appear in the parents column. `a-s00003063` does not appear linked to itself. (We correct for this in `subscriptions_xf` when introducing the `subscription_slug_for_counting` value and coalescing it with the slug.)

In the final CTE `finalish`, we join to intermediate to retreive the cohort dates before joining to `subscription_intermediate` in `subscription_xf`.

The end result of those same subscriptions:

|ULTIMATE_PARENT_SUB|CHILD_SUB|COHORT_MONTH|COHORT_QUARTER|COHORT_YEAR|
|:-:|:-:|:-:|:-:|:-:|
|a-s00003063|a-s00011816|2014-08-01|2014-07-01|2014-01-01|
|a-s00003063|a-s00011817|2014-08-01|2014-07-01|2014-01-01|
|a-s00003063|a-s00011818|2014-08-01|2014-07-01|2014-01-01|

This transformation process does not handle the consolidation of subscriptions, though, which is what `zuora_subscription_parentage_finish` picks up.

{% enddocs %}

{% docs prep_subscription_lineage_parentage_finish %}

This is the second part of a two-part model. (It is in two parts because of memory constraints.) For the first part, please checkout the docs for zuora_subscription_parentage_start.

Some accounts are not a direct renewal, they are the consolidation of many subscriptions into one. While the lineage model is build to accomodate these well, simply flattening the model produces one parent for many children accounts, for example:

|ULTIMATE_PARENT_SUB|CHILD_SUB|COHORT_MONTH|COHORT_QUARTER|COHORT_YEAR|
|:-:|:-:|:-:|:-:|:-:|
|a-s00003114|a-s00005209|2016-01-01|2016-01-01|2016-01-01|
|a-s00003873|a-s00005209|2017-01-01|2017-01-01|2017-01-01|

Since the whole point of ultimate parent is to understand cohorts, this poses a problem (not just for fan outs when joining) because it is inaccurate.

The `new_base` CTE identifies all affected subscriptions, while `consolidated_parents` and `deduped_parents` find the oldest version of the subscription.

This produces

|ULTIMATE_PARENT_SUB|CHILD_SUB|COHORT_MONTH|COHORT_QUARTER|COHORT_YEAR|
|:-:|:-:|:-:|:-:|:-:|
|a-s00003114|a-s00005209|2016-01-01|2016-01-01|2016-01-01|

but drops the subscriptions that are not the ultimate parent but had not previously been identified as children, in this case `a-s00003873`.

The first part of the `unioned` CTE isolates these subscriptions, naming them children of the newly-minted ultimate parent subscription (really just the oldest in a collection of related subscriptions), producing

|ULTIMATE_PARENT_SUB|CHILD_SUB|COHORT_MONTH|COHORT_QUARTER|COHORT_YEAR|
|:-:|:-:|:-:|:-:|:-:|
|a-s00003114|a-s00003873|2016-01-01|2016-01-01|2016-01-01|
|a-s00003114|a-s00003873|2016-01-01|2016-01-01|2016-01-01|


It unions this to the results of `deduped_consolidations` and all original base table where the subscriptions were not affected by consolidations. Finally we deduplicate one more time.  

The final result:

|ULTIMATE_PARENT_SUB|CHILD_SUB|COHORT_MONTH|COHORT_QUARTER|COHORT_YEAR|
|:-:|:-:|:-:|:-:|:-:|
|a-s00003114|a-s00009998|2016-01-01|2016-01-01|2016-01-01|
|a-s00003114|a-s00003873|2016-01-01|2016-01-01|2016-01-01|
|a-s00003114|a-s00005209|2016-01-01|2016-01-01|2016-01-01|


{% enddocs %}

{% docs prep_usage_ping %}
This data model is a prep model that supports a new dimension model, dim_usage_ping, that will replace PROD.legacy.version_usage_data, dim_usage_pings, version_usage_data_source, and version_raw_usage_data_source in the future . 

This is currently a WIP but inherits a lot of code from PROD.legacy.version_usage_data. See https://gitlab.com/gitlab-data/analytics/-/merge_requests/4064/diffs#bc1d7221ae33626053b22854f3ecbbfff3ffe633 for rationale. This curent version is for Sales team only. 

This is a sensitive model that should not be surfaced into Sisense because it contains IP Address. It also contains a mapping to remove the dependency on the IP Address. 

By the end of the data model, we have set up additional columns that clean up the data and ensures there is one SSOT for any given metric on a usage ping. 
{% enddocs %}


{% docs prep_usage_ping_no_license_key %}
This data model is a prep model that depends on prep_usage_ping and supports the creation of dim_usage_ping that will replace PROD.legacy.version_usage_data, dim_usage_pings, version_usage_data_source, and version_raw_usage_data_source in the future. 

This curent version is for Sales team only. 

Ideally, the purpose of this data model is to unpack all the metrics from the `raw_usage_data_payload` column, strips all the sensitive data out, and has one value for each metric in that column. 
{% enddocs %}


{% docs prep_usage_ping_saas_dates %}
This data model is a prep model that contains the dates of the usage pings for the self-managed instances that powers our SaaS GitLab.com data model. 
{% enddocs %}

{% docs prep_usage_ping_subscription_mapped %}
This data model is a prep model that depends on prep_usage_ping and supports the creation of dim_usage_ping that will replace PROD.legacy.version_usage_data, dim_usage_pings, version_usage_data_source, and version_raw_usage_data_source in the future. 

This curent version is for Sales team only. 

Ideally, the purpose of this data model is to identify the usage pings that can be mapped to a subscription. 
{% enddocs %}

{% docs prep_usage_ping_subscription_mapped_wave_2_3_metrics %}
The purpose of this data model is to identify the usage pings that can be mapped to a subscription and to unpack an initial set ([wave 2-3](https://gitlab.com/gitlab-data/analytics/-/blob/master/transform/snowflake-dbt/macros/version/sales_wave_2_3_metrics.sql)) of priority metrics from the `raw_usage_data_payload` column, strip all the sensitive data out, and then report one value for each metric in that column.

This data model is a prep model that depends on prep_usage_ping and supports the creation of dim_usage_ping that will replace `prod.legacy.version_usage_data`, `dim_usage_pings`, `version_usage_data_source`, and `version_raw_usage_data_source` in the future. 

This current version is for Sales team only. 

Ideally, the purpose of this data model is to identify the usage pings that can be mapped to a subscription and to unpack all the metrics from the `raw_usage_data_payload` column, strips all the sensitive data out, and has one value for each metric in that column. 

The metric list identifed can be found in the macro [`sales_wave_2_3_metrics`](https://gitlab.com/gitlab-data/analytics/-/blob/master/transform/snowflake-dbt/macros/version/sales_wave_2_3_metrics.sql).
{% enddocs %}

{% docs prep_gainsight_source_model_counts %}
This data model is used to capture the counts for all the source tables used for Gainsight. 

{% enddocs %}

{% docs prep_saas_usage_ping_subscription_mapped_wave_2_3_metrics %}

A recreation of `prep_usage_ping_subscription_mapped_wave_2_3_metrics` for _SaaS_ users.

{% enddocs %}

{% docs prep_saas_usage_ping_namespace %}

fct table from the usage_ping_namespace. Granularity of one row per namespace per metric per run

{% enddocs %}