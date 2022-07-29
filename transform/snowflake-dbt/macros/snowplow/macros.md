{% docs unpack_unstructured_event %}
This macro unpacks the unstructured snowplow events. It takes a list of field names, the pattern to match for the name of the event, and the prefix the new fields should use.
{% enddocs %}

{% docs clean_url %}
This macro will take a page_url_path string and use regex to strip out information which is not required for analysis. The regex explanation can be found here: https://regex101.com/r/nSdGr7/1
{% enddocs %}