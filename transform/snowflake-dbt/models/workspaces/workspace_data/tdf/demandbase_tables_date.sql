WITH demandbase_date AS (

    {% set tables = ['account','account_keyword_historical_rollup','account_keyword_intent','account_list','account_list_account','account_scores','account_site_page_metrics','campaign_account_performance','keyword_set','keyword_set_keyword'] %}	
                                                          
    {% for table in tables %} 
    SELECT '{{table}}'                                                            AS table_name,
        MAX(uploaded_at)                                                          AS max_date 
    FROM {{source('demandbase', table)}}  
  
  
    {% if not loop.last %}
    UNION ALL
    {% endif %}

{% endfor %} 
  
)


  SELECT *
  FROM demandbase_date 
  