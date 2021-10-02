WITH netsuite_date AS (

    {% set tables = ['accounting_books' , 'accounting_periods','accounts','classes','currencies','customers','departments','entity','subsidiaries','transaction_lines','vendors' ] %}	
                                                          
    {% for table in tables %} 
    SELECT '{{table}}'                                                            AS table_name,
        MAX(date_last_modified)                                                   AS max_date 
    FROM {{source('netsuite', table)}}  
  
  
    {% if not loop.last %}
    UNION ALL
    {% endif %}

{% endfor %} 
  
)


  SELECT *
  FROM netsuite_date 
  