{% docs map_bizible_marketing_channel_path %}
 Intermediate table to expose the mapped marketing channel data.
{% enddocs %}

{% docs map_bizible_campaign_grouping %}
 Mapping table for Bizible marketing campaigns which groups the campaigns based on the touchpoint type, ad campaign name, parent campaign id, among other attributes. This generates the `bizible_itegrated_campaign_grouping`, `integrated_campaign_grouping`, `touchpoint_segment`, and `gtm_motion` mapping.
{% enddocs %}

{% docs map_crm_account %}
 Mapping table for dimension keys related to crm accounts so they can be reused in fact tables containing account ids.
{% enddocs %}

{% docs map_crm_opportunity %}
 Mapping table for dimension keys related to opportunities so they can be reused in fact tables containing quotes.
{% enddocs %}

{% docs map_ip_to_country %}
Table for mapping ip address ranges to location ids.
{% enddocs %}

{% docs map_license_subscription_account%}
Table with mapping keys for license_md5 to subscription_id to crm (Salesforce) account ids
{% enddocs %}

{% docs map_merged_crm_account%}

Table mapping current crm account ids to accounts merged in the past.

{% enddocs %}

{% docs map_moved_duplicated_issue %}

Table mapping issues to the latest issue they were moved and / or duplicated to.

Example:

`Issue A` is moved to `Issue B`, `Issue B` is closed as duplicate of `Issue C`, `Issue C` is moved to `Issue D`

Then in our mapping table we would have:

| issue_id | dim_issue_id |
| -- | -- |
| Issue A | Issue D |
| Issue B | Issue D |
| Issue C | Issue D |
| Issue D | Issue D |

{% enddocs %}

{% docs map_namespace_lineage %}

Table containing GitLab namespace lineages. The primary goal of this table is to determine the ultimate parent namespace for all namespaces. Additionally, this table provides plan (GitLab subscription) information for both the given namespace and its ultimate parent namespace.

The grain of this table is one row per namespace. The Primary Key is `dim_namespace_id`.

{% enddocs %}

{% docs map_product_tier %}

 Table for mapping Zuora Product Rate Plans to Product Tier, Delivery Type, and Ranking.

{% enddocs %}

{% docs map_namespace_internal %}

This View contains the list of ultimate parent namespace ids that are internal to gitlab. In the future this list should be sourced from an upstream data sources or determined based on billing account in customer db if possible.

{% enddocs %}

{% docs map_team_member_bamboo_gitlab_dotcom_gitlab_ops %}
Table for mapping GitLab team members across bambooHR, GitLab.com Postgres DB, and GitLab Ops

{% enddocs %}

{% docs map_ci_runner_project %}
Table for mapping GitLab.com CI Runner to a specific project.

More info about [CI Runners here](https://docs.gitlab.com/ee/ci/runners/)
{% enddocs %}

{% docs map_usage_ping_active_subscription %}

Mapping table used to link a usage ping (dim_usage_ping_id) to an active zuora subscription at the ping creation date (dim_subscription_id).

This table is needed to identify how many active subscriptions send us on a month M sent us at least 1 usage ping. 
{% enddocs %}

{% docs map_subscription_opportunity %}

The distinct combination of subscriptions and opportunities generated through the rules defined in `prep_subscription_opportunity_mapping`. A flag has been created to indicate the subscription-opportunty mappings filled in by taking the most recent opportunity_id associated with a version of the subscription with the same subscription_name which we believe to have the lowest level of fidelity.

{% enddocs %}

