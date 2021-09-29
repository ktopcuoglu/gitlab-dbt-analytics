WITH snowplow_date AS (

    {% set tables = ['events' ,'events_sample','bad_events'] %}					

    {% for table in tables %} 
    SELECT '{{table}}'                                              AS table_name,
        MAX(uploaded_at)                                            AS max_date 
    FROM {{source('gitlab_snowplow', table)}}  
  
  
    {% if not loop.last %}
    UNION ALL
    {% endif %}

{% endfor %} 
  
)


  SELECT *
  FROM snowplow_date