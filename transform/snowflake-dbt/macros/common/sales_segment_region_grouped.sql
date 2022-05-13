{%- macro sales_segment_region_grouped(segment, sales_geo, sales_region) -%}

CASE 
  WHEN {{ segment }} IN ('Large', 'PubSec') AND {{ sales_geo }} = 'AMER' AND LOWER({{ sales_region }}) = 'west'
    THEN 'US West'
  WHEN {{ segment }} IN ('Large', 'PubSec') AND {{ sales_geo }} IN ('AMER', 'LATAM') AND LOWER({{ sales_region }}) IN ('east', 'latam')
    THEN 'US East'
  WHEN {{ segment }} IN ('Large', 'PubSec') AND {{ sales_geo }} IN ('APAC', 'PubSec','EMEA', 'Global')
    THEN {{ sales_geo }}
  WHEN {{ segment }} IN ('Large', 'PubSec') AND {{ sales_region }} = 'PubSec'
    THEN 'PubSec'
  WHEN {{ segment }} IN ('Large', 'PubSec') AND {{ sales_geo }} NOT IN ('West', 'East', 'APAC', 'PubSec','EMEA', 'Global')
    THEN 'Large Other'
  WHEN {{ segment }} NOT IN ('Large', 'PubSec')
    THEN {{ segment }}
  ELSE 'Missing segment_region_grouped'
END

{%- endmacro -%}
