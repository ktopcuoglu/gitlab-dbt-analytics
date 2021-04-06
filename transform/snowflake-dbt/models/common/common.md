{% docs bdg_crm_opportunity_contact_role %}

A fact table bridging opportunities with contacts. One opportunity can have multiple contacts and one can be flagged as the primary.

{% enddocs %}

{% docs bdg_namespace_order_subscription_active %}

The purpose of this table is two-fold:
1. Connect **Ultimate Parent** Namespace ID to Subscription (and hence Zuora billing account and CRM Account)
2. Connect Customer DB Customer ID to Subscription for self managed purchases. This helps with marketing efforts.

This table expands the functionality of the orders by improving the join to ultimate parent namespaces and subscriptions. Namespaces listed in this table are all Active with prior trials and currently paid plans. Subscriptions and Orders listed in this table are all SaaS and currently active.

The tier(s) connected to the subscription are determined using the underlying Zuora recurring charges. This view uses a `FULL OUTER JOIN` to show all three sides of the Venn diagram. (namespace, orders, subscriptions)
In doing so exceptions are noted within `namespace_order_subscription_match_status` to identify rows that do not match between systems.

{% enddocs %}

{% docs bdg_self_managed_order_subscription_active %}

The purpose of this table to connect Order IDs from Customer DB to Subscription for self managed purchases. This table expands the functionality of the subscriptions by improving the join to orders. Subscriptions and Orders listed in this table are all self-managed and currently active.

The tier(s) connected to the subscription are determined using the underlying Zuora recurring charges. This view uses a `FULL OUTER JOIN` to show all three parts of the Venn diagram (orders, subscriptions, and the overlap between the two).

{% enddocs %}

{% docs bdg_subscription_product_rate_plan %}
The goal of this table is to build a bridge from the entire "universe" of subscriptions in Zuora (`zuora_subscription_source` without any filters applied) to all of the [product rate plans](https://www.zuora.com/developer/api-reference/#tag/Product-Rate-Plan) to which those subscriptions are mapped. This provides the ability to filter subscriptions by delivery type ('SaaS' or 'Self-Managed').

{% enddocs %}

{% docs dim_alliance_type %}
Model to identify Channel partners that are alliance partners.

{% enddocs %}

{% docs dim_crm_account %}
Dimensional customer table representing all existing and historical customers from SalesForce. There are customer definitions for external reporting and additional customer definitions for internal reporting defined in the [handbook](https://about.gitlab.com/handbook/sales/#customer).

The Customer Account Management business process can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#1-customer-account-management-and-conversion-of-lead-to-opportunity).

The grain of the table is the SalesForce Account, also referred to as CRM_ID.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_crm_touchpoint %}
Descriptive fields for both attribution and non-attribution Bizible touchpoints.

{% enddocs %}

{% docs dim_crm_opportunity %}
Model for all dimensional opportunity columns from salesforce opportunity object

{% enddocs %}

{% docs dim_crm_person %}
Dimension that combines demographic data from salesforce leads and salesforce contacts. They are combined with a union and a filter on leads excluding converted leads and leads where there is a corresponding contact.

{% enddocs %}

{% docs dim_crm_sales_hierarchy_live %}
Dimension table representing the current state of the sales hierarchy, including the user segment, geo, region, and area as it is in the crm user object.

{% enddocs %}

{% docs dim_crm_sales_hierarchy_stamped %}
Dimension table representing the sales hierarchy at the time of a closed opportunity, including the user segment. These fields are stamped on the opportunity object on the close date and are used in sales funnel analyses.

{% enddocs %}

{% docs dim_billing_account %}
Dimensional table representing each individual Zuora account with details of person to bill for the account.

The Zuora account creation and maintenance is part of the broader Quote Creation business process and can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#3-quote-creation).

Data comes from [Zuora Documentation](https://www.zuora.com/developer/api-reference/#tag/Accounts).

The grain of the table is the Zuora Account.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_invoice %}

Dimension table providing invoice details at the single invoice grain.

The invoicing to customers business process can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#6-invoicing-to-customers).

Data comes from [Zuora Documentation](https://knowledgecenter.zuora.com/Billing/Reporting_and_Analytics/D_Data_Sources_and_Exports/C_Data_Source_Reference/Invoice_Item_Data_Source).

{% enddocs %}

{% docs dim_location_country %}

Dimensional table for countries mapped to larger regions.

{% enddocs %}

{% docs dim_location_region %}

Dimensional table for geographic regions.

{% enddocs %}

{% docs dim_product_detail %}
Dimensional table representing GitLab's Product Catalog. The Product Catalog is created and maintained through the Price Master Management business process and can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#2-price-master-management).

The Rate Plan Charge that is created on a customer account and subscription inherits its value from the Product Catalog.

Data comes from [Zuora Documentation](https://www.zuora.com/developer/api-reference/#tag/Product-Rate-Plan-Charges).

The grain of the table is the Product Rate Plan Charge.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_product_tier %}
Dimensional table representing [GitLab Tiers](https://about.gitlab.com/handbook/marketing/strategic-marketing/tiers/). Product [delivery type](https://about.gitlab.com/handbook/marketing/strategic-marketing/tiers/#delivery) and ranking are also captured in this table.

Data comes from [Zuora Documentation](https://www.zuora.com/developer/api-reference/#tag/Product-Rate-Plans).

The grain of the table is the Product Tier Name.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_subscription %}
Dimension table representing subscription details. The Zuora subscription is created and maintained as part of the broader Quote Creation business process and can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#3-quote-creation).

Data comes from [Zuora Documentation](https://www.zuora.com/developer/api-reference/#tag/Subscriptions).

The grain of the table is the version of a Zuora subscription.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_date %}
Dimensional table representing both calendar year and fiscal year date details.

The grain of the table is a calendar day.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_dr_partner_engagement %}
Model to identify the type of business engagement relationship a Partner has with GitLab.

{% enddocs %}

{% docs fct_campaign %}

Fact table representing marketing campaign details tracked in SFDC.

{% enddocs %}

{% docs fct_crm_attribution_touchpoint %}
Fact table for attribution Bizible touchpoints with shared dimension keys relating these touchpoints to dim_crm_person, dim_crm_opportunity, and dim_crm_account. These touchpoints have revenue associated with them.

{% enddocs %}

{% docs fct_crm_touchpoint %}
Fact table for non-attribution Bizible touchpoints with shared dimension keys relating these touchpoints to dim_crm_person and dim_crm_account.

{% enddocs %}

{% docs fct_crm_opportunity %}

A fact table for salesforce opportunities with keys to connect opportunities to shared dimensions through the attributes of the crm account.

{% enddocs %}

{% docs fct_crm_person %}

A fact table for Salesforce unconverted leads and contacts. The important stage dates have been included to calculate the velocity of people through the sales funnel. A boolean flag has been created to indicate leads and contacts who have been assigned a Marketo Qualified Lead Date, and a Bizible person id has been included to pull in the marketing channel based on the first touchpoint of a given lead or contact.

{% enddocs %}

{% docs fct_invoice %}

Fact table providing invoice details at the single invoice grain.

The invoicing to customers business process can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#6-invoicing-to-customers).

Data comes from [Zuora Documentation](https://knowledgecenter.zuora.com/Billing/Reporting_and_Analytics/D_Data_Sources_and_Exports/C_Data_Source_Reference/Invoice_Item_Data_Source).

{% enddocs %}

{% docs fct_invoice_item %}
Fact table providing invoice line item details.

The invoicing to customers business process can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#6-invoicing-to-customers).

Data comes from [Zuora Documentation](https://knowledgecenter.zuora.com/Billing/Reporting_and_Analytics/D_Data_Sources_and_Exports/C_Data_Source_Reference/Invoice_Item_Data_Source).

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_charge %}
Factual table with all rate plan charges coming from subscriptions or an amendment to a subscription.

Rate Plan Charges are created as part of the Quote Creation business process and can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#6-invoicing-to-customers).

Data comes from [Zuora Documentation](https://www.zuora.com/developer/api-reference/#tag/Rate-Plan-Charges).

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_licenses %}
Dimensional table representing generated licenses and associated metadata.

The grain of the table is a license_id.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_gitlab_dotcom_gitlab_emails %}
Dimensional table representing the best email address for GitLab employees from the GitLab.com data source

The grain of the table is a GitLab.com user_id.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}


{% docs dim_gitlab_ops_gitlab_emails %}
Dimensional table representing the best email address for GitLab team members from the Ops.GitLab.Net data source using the gitlab email address to identify GitLab team members

The grain of the table is a Ops.GitLab.Net user_id.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_gitlab_versions %}
Dimensional table representing released versions of GitLab.

The grain of the table is a version_id.

Additional information can be found on the [GitLab Releases](https://about.gitlab.com/releases/categories/releases/) page.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_quote_item %}

A fact table of quote amendments which have quotes and product rate plan charges associated with them. This model connected opportunities to quotes, quote amendments, and products.

{% enddocs %}

{% docs fct_quote %}

Fact table representing quotes pulled from the Zuora billing system. These are associated with crm accounts, billing accounts, opportunities, and subscriptions.

{% enddocs %}

{% docs fct_sales_funnel_target %}

Sales funnel targets set by the Finance team to measure performance of important KPIs against goals, broken down by sales hierarchy, and order attributes.

{% enddocs %}

{% docs fct_usage_data_monthly %}

Factual table derived from the metrics received as part of usage ping payloads.  

To create this table, all-time metrics are normalized to estimate usage over a month and combined with 28-day metrics.  Therefore, a single record in this table is one usage metric for a month for an instance of GitLab.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}


{% docs dim_crm_sales_rep %}

Dimension representing the associated sales rep from salesforce. Most often this will be the record owner, which is a ubiquitous field in salesforce.

{% enddocs %}

{% docs fct_usage_ping_subscription_mapped_gmau %}

This data model is at the **month | dim_subscription_id** grain for **Self-Managed** instances. In every month _that a Usage Ping payload was received_, for a given subscription, values of each GMAU and Paid GMAU metric from the last Usage Ping value in that month are reported.

This data model is used for the Customer Health Dashboards.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_usage_ping_subscription_mapped_smau %}

This data model is at the **month | dim_subscription_id** grain for **Self-Managed** instances. In every month _that a Usage Ping payload was received_, for a given subscription, values of each SMAU metric from the last Usage Ping value in that month are reported.

This data model is used for the Customer Health Dashboards.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_usage_ci_minutes %}

This table replicates the Gitlab UI logic that generates the CI minutes Usage Quota for both personal namespaces and top level group namespaces. The codebase logic used to build this model can be seen mapped in [this diagram](https://app.lucidchart.com/documents/view/0b8b66e6-8536-4a5d-b992-9e324581187d/0_0).

Namespaces from the `namespace_snapshots_monthly_all` CTE that are not present in the `namespace_statistics_monthly_all` CTE are joined into the logic with NULL `shared_runners_seconds` since these namespaces have not used CI Minutes on GitLab-provided shared runners. Since these CI Minutes are neither trackable nor monetizable, they can be functionally thought of as 0 `shared_runners_minutes_used_overall`. The SQL code has been implemented with this logic as justification.

It also adds two additional columns which aren't calculated in the UI, which are `limit_based_plan` and `status_based_plan` which are independent of whether there aren't projects with `shared_runners_enabled` inside the namespaces and only take into account how many minutes have been used from the monthly quota based in the plan of the namespace.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_namespace_member_summary %}

This model summarizes namespace user counts by accounting for all of the ways that a user be granted (direct or inherited) access to a namespace, AKA "membership". Bots and users awaiting access to a namespace are also accounted for. These counts are reported at the `ultimate_parent_namespace_id` grain.

Importantly, this model calculates the field [`billable_member_count`](https://docs.gitlab.com/ee/subscriptions/self_managed/#billable-users) - i.e. the number of members should be counted toward the seat count for a subscription (note: this also applies to namespaces without a subscription for the convenience of determining seats in use).

There are 5 general ways that a user can have access to a group A:
* Be a **group member** of group A.
* Be a **group member** of B, where B is a descendant (subgroup) of group A.
* Be a **project member** of b, where b is owned by A or one of A's descendants.
* Be a group member of N or a parent group of N, where N is invited to a project underneath A via [project group links](https://docs.gitlab.com/ee/user/group/#sharing-a-project-with-a-group).
* Be a group member of Y or a parent group of Y, where Y is invited to A or one of A's descendants via [group group links](https://docs.gitlab.com/ee/user/group/#sharing-a-group-with-another-group).

An example of these relationships is shown in this diagram:

<div style="width: 720px; height: 480px; margin: 10px; position: relative;"><iframe allowfullscreen frameborder="0" style="width:720px; height:480px" src="https://app.lucidchart.com/documents/embeddedchart/9f529269-3e32-4343-9713-8eb311df7258" id="WRFbB73aKeB3"></iframe></div>

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_product_usage_wave_1_3_metrics_latest %}
This table builds on the set of all Zuora subscriptions that are associated with a **Self-Managed** rate plans. Seat Link data from Customers DB (`fct_usage_self_managed_seat_link`) are combined with high priority Usage Ping metrics (`prep_usage_ping_subscription_mapped_wave_2_3_metrics`) to build out the set of facts included in this table. Only the most recently received Usage Ping and Seat Link per `dim_subscription_id` payload are reported included.

The data from this table will be used to create a mart table (`mart_product_usage_wave_1_3_metrics_latest`) for Gainsight Customer Product Insights.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_product_usage_wave_1_3_metrics_monthly %}
This table builds on the set of all Zuora subscriptions that are associated with a **Self-Managed** rate plans. Seat Link data from Customers DB (`fct_usage_self_managed_seat_link`) are combined with high priority Usage Ping metrics (`prep_usage_ping_subscription_mapped_wave_2_3_metrics`) to build out the set of facts included in this table. Only the most recently received Usage Ping and Seat Link payloads per `dim_subscription_id` each month are reported in this table.

The data from this table will be used to create a mart table (`mart_product_usage_wave_1_3_metrics_monthly`) for Gainsight Customer Product Insights.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_usage_ping_payloads %}
Factual table with metadata on usage ping payloads received.

The grain of the table is a usage_ping_id.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_usage_ping_metric_28_days %}
Factual table on the grain of an individual metric received as part of a usage ping payload.  This model specifically includes only metrics that represent usage over a month (or 28 days).

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_usage_ping_metric_all_time %}
Factual table on the grain of an individual metric received as part of a usage ping payload.  This model specifically includes only metrics that represent usage over the entire lifetime of the instance.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_usage_self_managed_seat_link %}

Self-managed EE instances send [Seat Link](https://docs.gitlab.com/ee/subscriptions/self_managed/#seat-link) usage data to [CustomerDot](https://gitlab.com/gitlab-org/customers-gitlab-com) on a daily basis. This information includes a count of active users and a maximum count of users historically in order to assist the [true up process](https://docs.gitlab.com/ee/subscriptions/self_managed/#users-over-license). Counts are reported from the last day of the month for historical months, and the most recent `reported_date` for the current month. Additional details can be found in [this doc](https://gitlab.com/gitlab-org/customers-gitlab-com/-/blob/staging/doc/reconciliations.md).

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_usage_storage %}
This table replicates the Gitlab UI logic that generates the Storage Usage Quotas for top level group namespaces. The logic used to build this model is explained in [this epic](https://gitlab.com/groups/gitlab-org/-/epics/4237). The specific front end logic is described [here](https://gitlab.com/groups/gitlab-org/-/epics/4237#note_400257377).

Storage usage is reported in bytes in source and this is reflected in the `_size` columns. These sizes are then converted into GiB (1 GiB = 2^30 bytes = 1,073,741,824 bytes), and MiB (1 MiB = 2^20 bytes = 1,048,576 bytes), which is most often displayed in the UI. Since storage limits are allocated in GiB, they were left as such in the `_limit` columns.

Since this table reports at the top level namespace grain, aggregation of the individual underlying repositories is required. To increase visibility of the underlying repositories, two count columns (and their associated flags) are added that aren't calculated in the UI: which are `repositories_above_free_limit_count` and `capped_repositories_count`. These columns can serve as helpful indicators for when a customer will likely need to purchase extra storage.

For the purpose of this table, all child namespaces under a top level namespace with unlimited storage are also assumed to have unlimited storage. Also, storage sizes are converted to MiB and GiB in this table because these are the values being reported under the hood, even though on a project page storage is reported as "MB" or "GB".

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_usage_pings %}
Dimension that contains demographic data from usage ping data, including additional breaks out for product_tier, if it is from an internal instance, and replaces the ip_address hash with a location_id instead.

[Core represents both CE and EE](https://about.gitlab.com/handbook/marketing/product-marketing/tiers/#history-of-ce-and-ee-distributions).

Get started by exploring the [Product Geolocation Analysis](https://about.gitlab.com/handbook/business-ops/data-team/data-catalog/product-geolocation/) handbook page.
Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_host_instance_type %}

Dimension table providing instance type for a given UUID/Host Name pair.

{% enddocs %}

{% docs dim_instances %}
Dimension that contains statistical data for instances from usage ping data

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_sales_qualified_source %}

Sales qualified source dimension, based off of salesforce opportunity data, using the `generate_single_field_dimension` macro to create the final formatted SQL

{% enddocs %}

{% docs dim_deal_path %}

Deal path dimension, based off of salesforce opportunity data, using the `generate_single_field_dimension` macro to create the final formatted SQL

{% enddocs %}

{% docs dim_bizible_marketing_channel_path %}

Bizible marketing channel path dimension, based off a grouping of Bizible marketing channel paths in `map_bizible_marketing_channel_path`.

{% enddocs %}

{% docs dim_sales_segment %}

Dimension table for sales segment built off Ultimate_Parent_Sales_Segment_Employees__c in SFDC field in account data. Example values: SMB, Mid-Market, Large

{% enddocs %}


{% docs dim_sales_territory %}

Sales territory dimension, based off of salesforce account data, using the `generate_single_field_dimension_from_prep` macro to create the final formatted SQL

{% enddocs %}

{% docs dim_industry %}

Industry dimension, based off of salesforce account data, using the `generate_single_field_dimension_from_prep` macro to create the final formatted SQL

{% enddocs %}

{% docs dim_order_type %}

Order type dimension, based off of salesforce opportunity data, using the `generate_single_field_dimension` macro to create the final formatted SQL

{% enddocs %}

{% docs dim_namespace%}

Includes all columns from the namespaces base model. The plan columns in this table (gitlab_plan_id, gitlab_plan_title, gitlab_plan_is_paid) reference the plan that is inheritted from the namespace's ultimate parent.

This table add a count of members and projects currently associated with the namespace.
Boolean columns: gitlab_plan_is_paid, namespace_is_internal, namespace_is_ultimate_parent

A NULL namespace type defaults to "Individual".
This table joins to common product tier dimension via dim_product_tier_id to get the current product tier.

{% enddocs %}

{% docs dim_quote %}

Dimensional table representing Zuora quotes and associated metadata.

The grain of the table is a quote_id.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_license %}

Dimensional table representing generated licenses and associated metadata.

The grain of the table is a license_id.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_key_xmau_metric %}

A fact table that contains only the metrics that is a UMAU, SMAU, or GMAU metric that appears on the [Stages and Groups Performance Indicator handbook page](https://about.gitlab.com/handbook/product/stage-and-group-performance-indicators/)

{% enddocs %}
