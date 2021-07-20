{% docs email_domain_type %}

This macro classifies email_domains in 4 buckets:

1. **Bulk load or list purchase or spam impacted**: Based on the lead_source.
1. **Personal email domain**: Based on full and partial matching using a list of personal email domains returned by the get_personal_email_domain_list macro.
1. **Business email domain**: All non NULL email_domains that do not classify as Bulk or Personal.
1. **Missing email domain**: Empty email_domains.

{% enddocs %}

{% docs it_job_title_hierarchy %}

This macro maps a job title to the IT job title hierarchy. It works by doing string matching on the job title and categorizing them into 3 buckets:

1. **IT Decision Makers**: CIO, CTO, VP of IT, ...
2. **IT Managers**: Manager of IT. Manager of Procurement, ...
3. **IT Individual contributors**: Software Developer, Application Developer, IT programmer, ...

These buckets are only for IT, information systems, engineering, ... Everything else gets a NULL value assigned to it.

This macro uses the pad_column macro to "pad" the job title field with spaces and discard unrelated pattern matching.

An example of this is the matching for the job title of `IT Manager`. The string pattern for it `%it%manager%` also gets unrelated matches like `Junior Digital Project Manager` or `Supplier Quality Section Manager`. To overcome this problem, the job title field is "padded" with spaces to the both sides of the string and the string pattern changed `% it%manager%`. This way the previous unrelated job titles would not match.

{% enddocs %}

{% docs map_marketing_channel_path %}
This macro maps channel path to the marketing channel name.
{% enddocs %}
