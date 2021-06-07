{% docs it_job_title_hierarchy %}

This macro maps a job title to the IT job title hierarchy. It works by doing string matching on the job title and categorizing them into 3 buckets:

1. **Decision Makers**: CIO, CTO, VP of IT, ...
2. **Managers**: Manager of IT. Manager of Procurement, ...
3. **Individual contributors**: Software Developer, Application Developer, IT programmer, ...

These buckets are only for IT, information systems, engineering, ... Everything else gets a NULL value assigned to it.

{% enddocs %}

{% docs map_marketing_channel_path %}
This macro maps channel path to the marketing channel name.
{% enddocs %}
