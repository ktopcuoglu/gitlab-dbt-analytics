WITH versiondb_date AS (

    {% set tables = ['conversational_development_indices', 'fortune_companies', 'hosts',  'usage_data'] %}					

    {% for table in tables %} 
    SELECT '{{table}}'                                               AS table_name,
        MAX(updated_at)                                              AS max_date 
    FROM {{source('version', table)}}  
  
  
    {% if not loop.last %}
    UNION ALL
    {% endif %}

{% endfor %} 
  
UNION ALL
  
      {% set tables = [ 'raw_usage_data', 'versions', 'version_checks'] %}					

    {% for table in tables %} 
    SELECT '{{table}}'                                               AS table_name,
        MAX(created_at)                                              AS max_date 
    FROM {{source('version', table)}}  
  
  
    {% if not loop.last %}
    UNION ALL
    {% endif %}

{% endfor %} 
  
)

  SELECT *
  FROM versiondb_date