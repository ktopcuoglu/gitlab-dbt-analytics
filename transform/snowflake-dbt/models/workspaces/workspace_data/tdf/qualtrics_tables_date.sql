WITH qualtrics_date AS (

    {% set tables = [
'contact','distribution','nps_survey_responses','post_purchase_survey_responses','survey','questions' ] %}
        
                                                          
    {% for table in tables %} 
    SELECT '{{table}}'                                                     AS table_name,
        MAX(uploaded_at)                                                   AS max_date 
    FROM {{source('qualtrics', table)}}  
  
  
    {% if not loop.last %}
    UNION ALL
    {% endif %}

{% endfor %} 
  
)


  SELECT *
  FROM qualtrics_date 
  