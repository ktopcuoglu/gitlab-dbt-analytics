{%- macro sales_segment_region_grouped(segment, sales_region) -%}

CASE 
  WHEN {{ segment }} IN ('Large', 'PubSec') AND {{ sales_region }} = 'West'
    THEN 'US West'
  WHEN {{ segment }} IN ('Large', 'PubSec') AND {{ sales_region }} IN ('East', 'LATAM')
    THEN 'US East'
  WHEN {{ segment }} IN ('Large', 'PubSec') AND {{ sales_region }} IN ('APAC', 'PubSec','EMEA', 'Global')
    THEN {{ sales_region }}
  WHEN {{ segment }} IN ('Large', 'PubSec') AND {{ sales_region }} NOT IN ('West', 'East', 'APAC', 'PubSec','EMEA', 'Global')
    THEN 'Other'
  WHEN {{ segment }} NOT IN ('Large', 'PubSec')
    THEN {{ segment }}
  ELSE 'Missing segment_region_grouped'
END

{%- endmacro -%}
