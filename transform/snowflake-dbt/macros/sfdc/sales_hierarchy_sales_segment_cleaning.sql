{%- macro sales_hierarchy_sales_segment_cleaning(column_1) -%}

CASE WHEN lower({{ column_1 }}) = 'smb' THEN 'SMB'
     WHEN lower({{ column_1 }}) LIKE ('mid%market') THEN 'Mid-Market'
     WHEN lower({{ column_1 }}) = 'public sector' THEN 'PubSec'
     WHEN lower({{ column_1 }}) = 'pubsec' THEN 'PubSec'
     WHEN lower({{ column_1 }}) = 'unknown' THEN 'SMB'
     WHEN lower({{ column_1 }}) IS NULL THEN 'SMB'
     WHEN {{ column_1 }} IS NOT NULL THEN {{ column_1 }}
END

{%- endmacro -%}
