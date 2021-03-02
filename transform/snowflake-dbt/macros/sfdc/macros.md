{% docs sales_hierarchy_sales_segment_cleaning%}
This macro is a CASE WHEN statement that applies formatting to the sales hierarchy sales segment taken from both the user and opportunity objects to form the live and stamped sales hierarchy.
{% enddocs %}

{% docs sales_qualified_source_cleaning %}
This macro applies correct mapping to the sales_qualified_source field. This is mapping BDR Generated sales qualified source to SDR Generated.
{% enddocs %}

{% docs sales_segment_cleaning %}
This macro applies proper formatting to sales segment data with the end result being one of SMB, Mid-Market, Strategic, Large or Unknown.
{% enddocs %}

{% docs sfdc_deal_size %}
This macro buckets a unit into a deal size (Small, Medium, Big, or Jumbo) based on an inputted value.
{% enddocs %}

{% docs sfdc_source_buckets %}
This macro is a CASE WHEN statement that groups the lead sources into new marketing-defined buckets. @rkohnke is the DRI on any changes made to this macro.
{% enddocs %}

{% docs sales_funnel_text_slugify %}
This macro is a removes spaces, special characters, and capital letters from text fields taken from a sheetload filled out by the Finance team to complete the sales funnel targets.
{% enddocs %}
