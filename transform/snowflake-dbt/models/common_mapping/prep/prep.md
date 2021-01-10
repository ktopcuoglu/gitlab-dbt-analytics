{% docs prep_sfdc_account %}

SFDC Account Prep table, used to clean and dedupe fields from a common source for use in further downstream dimensions.
Cleaning operations vary across columns, depending on the nature of the source data. See discussion in [MR](https://gitlab.com/gitlab-data/analytics/-/merge_requests/3782) for further details

{% enddocs %}

{% docs prep_crm_sales_hierarchy_live %}

Creates a base view with generated keys for the live crm sales hierarchy shared dimensions and references in facts.

{% enddocs %}

{% docs prep_crm_sales_hierarchy_stamped %}

Creates a base view with generated keys for the stamped/historical crm sales hierarchy shared dimensions and references in facts. This is built from the stamped fields in the opportunity object and will be used in sales funnel analyses.

{% enddocs %}

{% docs prep_geo_area %}

Creates a base view with generated keys for the geo_area shared dimension and references in facts.

{% enddocs %}

{% docs prep_geo_region %}

Creates a base view with generated keys for the geo_region shared dimension and references in facts.

{% enddocs %}

{% docs prep_geo_sub_region %}

Creates a base view with generated keys for the geo_sub_region shared dimension and references in facts.

{% enddocs %}

{% docs prep_industry %}

Creates a base view with generated keys for the industry shared dimension and references in facts.

{% enddocs %}

{% docs prep_location_region %}

Creates a base view with generated keys for the geographic region shared dimension and references in facts.

{% enddocs %}

{% docs prep_marketing_channel %}

Creates a base view with generated keys for the marketing channel shared dimension and references in facts.

{% enddocs %}

{% docs prep_opportunity_source %}

Creates a base view with generated keys for the opportunity source shared dimension and references in facts.

{% enddocs %}

{% docs prep_order_type %}

Creates a base view with generated keys for the order type shared dimension and references in facts.

{% enddocs %}

{% docs prep_purchase_channel %}

Creates a base view with generated keys for the purchase channel shared dimension and references in facts.

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

{% docs prep_product_tier %}

 This table creates keys for the common product tier dimension that is used across gitlab.com and Zuora data sources. 

 The granularity of the table is product tier.

{% enddocs %}

{% docs prep_quote %}

Creates a Quote Prep table for representing Zuora quotes and associated metadata for shared dimension and references in facts.

The grain of the table is a quote_id.

{% enddocs %}

