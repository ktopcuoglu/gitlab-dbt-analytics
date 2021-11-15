{% docs prep_alliance_type %}

Creates a base view with generated keys for the alliance type shared dimension and references in facts.

{% enddocs %}

{% docs prep_audit_event_details_clean %}

All GitLab audit event details, with pii replaced with hashes. Created by a union of audit event keys from `gitlab_dotcom_audit_event_details` and `gitlab_dotcom_audit_event_details_pii`.

{% enddocs %}

{% docs prep_dr_partner_engagement %}

Creates a base view with generated keys for the dr partner engagement shared dimension and references in facts.

{% enddocs %}

{% docs prep_epic_user_request_collaboration_project %}

Parses epic links to the `Gitlab-org` group in the description and notes of epics inside the customer collaboration projects. These epics links are related to user feature requests from the product.

{% enddocs %}

{% docs prep_epic_user_request %}

Parses SFDC Opportunity / Accounts and Zendesk tickets links in the description and notes of epics inside the `Gitlab-org` group, together with its priority represented by the label `~"customer priority::[0-10]"` . These epics are related to user feature requests from the product.

For Opportunity and Zendesk tickets links found, the associated SFDC Account id is filled into the record.

If the same link is found twice in the description and the notes of the same epic, then the link that will be taken, together with its priority, will be the one in the note. If the same link is found in two different notes in the same epic, then the link that will be taken, together with its priority, will be the one in the latest updated note.

This model assumes that only one priority is placed in a given description or note.

{% enddocs %}

{% docs prep_issue_user_request_collaboration_project %}

Parses issue links to the `Gitlab-org` group in the description and notes of issues inside the customer collaboration projects. These issues links are related to user feature requests from the product.

It also looks for the issue links to the `Gitlab-org` group in the related issue links.

{% enddocs %}

{% docs prep_issue_user_request %}

Parses SFDC Opportunity / Accounts and Zendesk tickets links in the description and notes of issues inside the `Gitlab-org` group, together with its priority represented by the label `~"customer priority::[0-10]"` . These issues are related to user feature requests from the product.

For Opportunity and Zendesk tickets links found, the associated SFDC Account id is filled into the record.

If the same link is found twice in the description and the notes of the same issue, then the link that will be taken, together with its priority, will be the one in the note. If the same link is found in two different notes in the same issue, then the link that will be taken, together with its priority, will be the one in the latest updated note.

This model assumes that only one priority is placed in a given description or note.

{% enddocs %}

{% docs prep_sfdc_account %}

SFDC Account Prep table, used to clean and dedupe fields from a common source for use in further downstream dimensions.
Cleaning operations vary across columns, depending on the nature of the source data. See discussion in [MR](https://gitlab.com/gitlab-data/analytics/-/merge_requests/3782) for further details

{% enddocs %}

{% docs prep_campaign %}

Creates a base view with generated keys for the campaign shared dimension and fact and references in facts.

{% enddocs %}

{% docs prep_crm_user %}

Creates a base view with generated keys for the user and live crm sales hierarchy shared dimensions and references in facts.

{% enddocs %}

{% docs prep_crm_user_hierarchy_live %}

Creates a base view with generated keys for the live crm user hierarchy shared dimensions and references in facts.

{% enddocs %}

{% docs prep_crm_user_hierarchy_stamped %}

Creates a base view with generated keys for the stamped/historical crm user hierarchy shared dimensions and references in facts. This is built from the stamped fields in the opportunity object and will be used in sales funnel analyses.

{% enddocs %}

{% docs prep_gitlab_dotcom_application_settings_monthly %}

This model captures a historical record of GitLab's default application settings for CI minutes and storage at a monthly grain.

{% enddocs %}

{% docs prep_gitlab_dotcom_plan %}

Creates a base view with generated keys for the plans shared dimension and fact and references in facts.

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

{% docs prep_charge %}

Creates a base view of recurring charges that are not amortized over the months. This prep table is used for transaction line analyses that do not require amortization of charges.

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

{% docs prep_saas_usage_ping_free_user_metrics %}

Table containing **free** SaaS users in preparation for free user usage ping metrics fact table.

The grain of this table is one row per namespace per month.

{% enddocs %}

{% docs prep_usage_ping_free_user_metrics %}

Table containing **free** Self-Managed users in preparation for free user usage ping metrics fact table.

The grain of this table is one row per uuid-hostname combination per month.

{% enddocs %}

{% docs prep_ci_pipeline %}

Creates a base view of CI pipelines. More info about CI pipelines [is available here](https://docs.gitlab.com/ee/ci/pipelines/)

{% enddocs %}

{% docs prep_action %}

Prep table for the dim table `dim_action`.

More info about [events tracked](https://docs.gitlab.com/ee/api/events.html)

{% enddocs %}

{% docs prep_user %}
Prep table for the dim table `dim_user`.

This table is currently the first iteration. This is a relatively narrow table. A lot of metadata needs to be added.

{% enddocs %}

{% docs prep_issue %}

Prep table used to build `dim_merge_request`

More information about [Issues](https://docs.gitlab.com/ee/user/project/issues/)

{% enddocs %}

{% docs prep_merge_request %}

Prep table used to build `dim_merge_request`

More information about [CI Pipelines here](https://docs.gitlab.com/ee/user/project/merge_requests/)

{% enddocs %}

{% docs prep_ci_build %}

Prep table used to build the `dim_ci_build` table.

More information about [CI Pipelines here](https://docs.gitlab.com/ee/ci/pipelines/)

{% enddocs %}

{% docs prep_ci_runner %}

Prep table used to build the `dim_ci_runner` table.

More information about [CI Pipelines here](https://docs.gitlab.com/ee/ci/pipelines/)

{% enddocs %}

{% docs prep_epic %}

Prep table for the dim table `dim_epic`.

{% enddocs %}

{% docs prep_note %}

Prep table for the dim table `dim_note`.

{% enddocs %}


{% docs prep_monthly_usage_data_28_days %}

This model transforms 28_days counters stored in the usage data payload into monthly proxies. To do so the logic is as follows:

* For one specific instance (instance_uid), one specific metric (metrics_name) and one specific month M, take the latest received data

{% enddocs %}

{% docs prep_monthly_usage_data_all_time %}

This model transforms all_time counters stored in the usage data payload into monthly proxies. To do so the logic is as follows:

* For one specific instance (instance_uid), one specific metric (metrics_name) and one specific month M, take the latest received data
* Compare this data point with the one received one month before, we have 3 scenarios:
  * data from month M > data from month M-1: the monthly proxied metric is data(M) - data(M-1)
  * no data for month M-1: then monthly proxied metric is data(M)
  * data from month M < data from month M-1: abnormal behavior which might be due to an instance reset, then proxied metric is 0

{% enddocs %}

{% docs prep_usage_data_7_days_flattened %}

The granularity of this model is one row per tuple (metric_name, instance_id).

Usage ping's data is stored in several nested jsons as shown in [this page](https://docs.gitlab.com/ee/development/telemetry/usage_ping.html#example-usage-ping-payload).

Those metrics sent could be of various types:
* all_time counters (for example how many issues a specific instance has created since its inception)
* 28_days counters (how many users have created at least one issue over the last 4 weeks)
* 7_days counters (how many users have created at least one issue over the last 7 days)
* an instance configuration parameter (has this instance enabled saml/sso)

This model extracts the 7-days counters (based on the mapping table in this spreadsheet) and flattens the json.

The models transforms this json:

```
{
  "uuid": "0000000-0000-0000-0000-000000000000",
  "hostname": "example.com",
  "version": "12.10.0-pre",
  ...
  "analytics_unique_visits": {
    "g_analytics_contribution": 999,
    ...
  },
  "usage_activity_by_stage_monthly": {
    "configure": {
      "project_clusters_enabled": 999,
      ...
    },
    "create": {
      "merge_requests": 999,
      ...
    },
    "manage": {
      "events": 999,
      ...
    }
  }
  }
```

into this table:

| uuid                                | hostname    | version     | full_metric_path                                                   | clean_metric_name                                      | metric_value |
|-------------------------------------|-------------|-------------|--------------------------------------------------------------------|--------------------------------------------------------|--------------|
| 0000000-0000-0000-0000-000000000000 | example.com | 12.10.0-pre | analytics_unique_visits.g_analytics_contribution                   | g_analytics_contribution_unique_visitors_count_28_days | 999          |
| 0000000-0000-0000-0000-000000000000 | example.com | 12.10.0-pre | usage_activity_by_stage_monthly.configure.project_clusters_enabled | project_clusters_ebled_users_count_28_days                     | 999          |
| 0000000-0000-0000-0000-000000000000 | example.com | 12.10.0-pre | usage_activity_by_stage_monthly.create.merge_requests              | merge_requests_creation_users_count_28_days            | 999          |
| 0000000-0000-0000-0000-000000000000 | example.com | 12.10.0-pre | usage_activity_by_stage_monthly.manage.events                      | events_users_count_28_days                             | 999          |


{% enddocs %}

{% docs prep_usage_data_28_days_flattened %}

The granularity of this model is one row per tuple (metric_name, instance_id).

Usage ping's data is stored in several nested jsons as shown in [this page](https://docs.gitlab.com/ee/development/telemetry/usage_ping.html#example-usage-ping-payload).

Those metrics sent could be of various types:
* all_time counters (for example how many issues a specific instance has created since its inception)
* 7_days counters (how many users have created at least one issue over the last 4 weeks)
* an instance configuration parameter (has this instance enabled saml/sso)

This model extracts the 28-days counters (based on the mapping table in this spreadsheet) and flattens the json.

The models transforms this json:

```
{
  "uuid": "0000000-0000-0000-0000-000000000000",
  "hostname": "example.com",
  "version": "12.10.0-pre",
  ...
  "analytics_unique_visits": {
    "g_analytics_contribution": 999,
    ...
  },
  "usage_activity_by_stage_monthly": {
    "configure": {
      "project_clusters_enabled": 999,
      ...
    },
    "create": {
      "merge_requests": 999,
      ...
    },
    "manage": {
      "events": 999,
      ...
    }
  }
  }
```

into this table:

| uuid                                | hostname    | version     | full_metric_path                                                   | clean_metric_name                                      | metric_value |
|-------------------------------------|-------------|-------------|--------------------------------------------------------------------|--------------------------------------------------------|--------------|
| 0000000-0000-0000-0000-000000000000 | example.com | 12.10.0-pre | analytics_unique_visits.g_analytics_contribution                   | g_analytics_contribution_unique_visitors_count_28_days | 999          |
| 0000000-0000-0000-0000-000000000000 | example.com | 12.10.0-pre | usage_activity_by_stage_monthly.configure.project_clusters_enabled | project_clusters_ebled_users_count_28_days                     | 999          |
| 0000000-0000-0000-0000-000000000000 | example.com | 12.10.0-pre | usage_activity_by_stage_monthly.create.merge_requests              | merge_requests_creation_users_count_28_days            | 999          |
| 0000000-0000-0000-0000-000000000000 | example.com | 12.10.0-pre | usage_activity_by_stage_monthly.manage.events                      | events_users_count_28_days                             | 999          |


{% enddocs %}

{% docs prep_usage_data_all_time_flattened %}

The granularity of this model is one row per tuple (metric_name, instance_id).

Usage ping's data is stored in several nested jsons as shown in [this page](https://docs.gitlab.com/ee/development/telemetry/usage_ping.html#example-usage-ping-payload).

Those metrics sent could be of various types:
* all_time counters (for example how many issues a specific instance has created since its inception)
* 28_days counters (how many users have created at least one issue over the last 4 months)
* an instance configuration parameter (has this instance enabled saml/sso)

This model extracts the all-time counters (based on the mapping table in this spreadsheet) and flattens the json.

The models transforms this json:

```
{
  "uuid": "0000000-0000-0000-0000-000000000000",
  "hostname": "example.com",
  "version": "12.10.0-pre",
  ...
  "analytics_unique_visits": {
    "g_analytics_contribution": 999,
    ...
  },
  "usage_activity_by_stage_monthly": {
    "configure": {
      "project_clusters_enabled": 999,
      ...
    },
    "create": {
      "merge_requests": 999,
      ...
    },
    "manage": {
      "events": 999,
      ...
    }
  }
  }
```

into this table:

| uuid                                | hostname    | version     | full_metric_path                                                   | clean_metric_name                                      | metric_value |
|-------------------------------------|-------------|-------------|--------------------------------------------------------------------|--------------------------------------------------------|--------------|
| 0000000-0000-0000-0000-000000000000 | example.com | 12.10.0-pre | analytics_unique_visits.g_analytics_contribution                   | g_analytics_contribution_unique_visitors_count_all_time | 999          |
| 0000000-0000-0000-0000-000000000000 | example.com | 12.10.0-pre | usage_activity_by_stage_monthly.configure.project_clusters_enabled_all_time | project_clusters_enabled_users_count_all_time                     | 999          |
| 0000000-0000-0000-0000-000000000000 | example.com | 12.10.0-pre | usage_activity_by_stage_monthly.create.merge_requests              | merge_requests_creation_users_count_all_time            | 999          |
| 0000000-0000-0000-0000-000000000000 | example.com | 12.10.0-pre | usage_activity_by_stage_monthly.manage.events                      | events_users_count_all_time                            | 999          |

{% enddocs %}

{% docs prep_usage_ping_payload %}

prep table used to build fct_usage_ping_payload.

{% enddocs %}

{% docs prep_usage_ping_settings %}

prep table selecting for a specific usage ping a list of metadata related to specific instance settings. Any metric which is not based on timeseries could be  selected.
{% enddocs %}
{% docs prep_deployment %}

Prep table for the dim table `dim_deployment` that is not yet created.

{% enddocs %}

{% docs prep_package %}

Prep table for the dim table `dim_package` that is not yet created. It is also used in the `prep_event` table

{% enddocs %}

{% docs prep_issue_severity %}

Prep table used to get Severity field from GitLab Incident issues for the `dim_issue` table.

More information about [GitLab Incidents here](https://docs.gitlab.com/ee/operations/incident_management/incidents.html)

{% enddocs %}

{% docs prep_label_links %}

Prep table used to join GitLab Labels to Issues, Merge Requests, & Epics

More information about [labels here](https://docs.gitlab.com/ee/user/project/labels.html)

{% enddocs %}

{% docs prep_labels %}

Prep table used to build `dim_issues`, `dim_merge_requests`, `dim_epics` tables. Holds detailed information about the labels used across GitLab

More information about [labels here](https://docs.gitlab.com/ee/user/project/labels.html)

{% enddocs %}

{% docs prep_issue_links %}

Prep table used to build `dim_issue_links` This table shows relationships of GitLab issues to other GitLab issues. It represents linked issues, which you can learn more about [here](https://docs.gitlab.com/ee/user/project/issues/related_issues.html)

{% enddocs %}

{% docs prep_release %}

Prep table for the dim table `dim_release` that is not yet created. It is also used in the `prep_event` table

{% enddocs %}

{% docs prep_requirement %}

Prep table for the dim table `dim_requirement` that is not yet created. It is also used in the `prep_event` table
{% enddocs %}
