{% docs bdg_crm_opportunity_contact_role %}

A fact table bridging opportunities with contacts. One opportunity can have multiple contacts and one can be flagged as the primary.

{% enddocs %}

{% docs dim_crm_account %}
Dimensional customer table representing all existing and historical customers from SalesForce. There are customer definitions for external reporting and additional customer definitions for internal reporting defined in the [handbook](https://about.gitlab.com/handbook/sales/#customer).

The Customer Account Management business process can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#1-customer-account-management-and-conversion-of-lead-to-opportunity).

The grain of the table is the SalesForce Account, also referred to as CRM_ID.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_crm_opportunity %}
Model for all dimensional opportunity columns from salesforce opportunity object

{% enddocs %}

{% docs dim_crm_person %}
Dimension that combines demographic data from salesforce leads and salesforce contacts. They are combined with a union and a filter on leads excluding converted leads and leads where there is a corresponding contact.

{% enddocs %}

{% docs dim_billing_account %}
Dimensional table representing each individual Zuora account with details of person to bill for the account.

The Zuora account creation and maintenance is part of the broader Quote Creation business process and can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#3-quote-creation).

Data comes from [Zuora Documentation](https://www.zuora.com/developer/api-reference/#tag/Accounts).

The grain of the table is the Zuora Account.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_ip_to_geo %}

Dimensional table mapping ip address ranges to location ids.

{% enddocs %}

{% docs dim_location %}

Dimensional table for geo locations.

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

{% docs dim_product_details %}
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

{% docs dim_subscriptions %}
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

{% docs fct_campaign %}

Fact table representing marketing campaign details tracked in SFDC.

{% enddocs %}

{% docs fct_crm_opportunity %}

A fact table for salesforce opportunities with keys to connect opportunities to shared dimensions through the attributes of the crm account.

{% enddocs %}

{% docs fct_crm_person %}

A fact table for Salesforce unconverted leads and contacts. The important stage dates have been included to calculate the velocity of people through the sales funnel. A boolean flag has been created to indicate leads and contacts who have been assigned a Marketo Qualified Lead Date, and a Bizible person id has been included to pull in the marketing channel based on the first touchpoint of a given lead or contact.

{% enddocs %}

{% docs fct_invoice_items %}
Fact table providing invoice line item details.

The invoicing to customers business process can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#6-invoicing-to-customers).

Data comes from [Zuora Documentation](https://knowledgecenter.zuora.com/Billing/Reporting_and_Analytics/D_Data_Sources_and_Exports/C_Data_Source_Reference/Invoice_Item_Data_Source).

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_charges %}
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


{% docs fct_usage_data_monthly %}

Factual table derived from the metrics received as part of usage ping payloads.  

To create this table, all-time metrics are normalized to estimate usage over a month and combined with 28-day metrics.  Therefore, a single record in this table is one usage metric for a month for an instance of GitLab.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}


{% docs dim_crm_sales_rep %}

Dimension representing the associated sales rep from salesforce. Most often this will be the record owner, which is a ubiquitous field in salesforce.

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

{% docs dim_usage_pings %}
Dimension that contains demographic data from usage ping data, including additional breaks out for product_tier, if it is from an internal instance, and replaces the ip_address hash with a location_id instead.

[Core represents both CE and EE](https://about.gitlab.com/handbook/marketing/product-marketing/tiers/#history-of-ce-and-ee-distributions).

Get started by exploring the [Product Geolocation Analysis](https://about.gitlab.com/handbook/business-ops/data-team/data-catalog/product-geolocation/) handbook page.
Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_instances %}
Dimension that contains statistical data for instances from usage ping data

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_opportunity_source %}

Opportunity source dimension, based off of salesforce opportunity data, using the `generate_single_field_dimension` macro to create the final formatted SQL

{% enddocs %}

{% docs dim_purchase_channel %}

Purchase channel dimension, based off of salesforce opportunity data, using the `generate_single_field_dimension` macro to create the final formatted SQL

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