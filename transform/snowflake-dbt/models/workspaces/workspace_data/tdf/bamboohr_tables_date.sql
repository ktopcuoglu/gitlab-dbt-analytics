WITH bamboohr_date AS (

    {% set tables = ['compensation', 'custom_currency_conversion', 'custom_currency_conversion', 'directory', 'emergency_contacts', 'employment_status', 'employment_status', 'job_info', 'id_employee_number_mapping', 'meta_fields'] %}	
                                                          
    {% for table in tables %} 
    SELECT '{{table}}'                                                            AS table_name,
        MAX(uploaded_at)                                                          AS max_date 
    FROM {{source('bamboohr', table)}}  
  
  
    {% if not loop.last %}
    UNION ALL
    {% endif %}

{% endfor %} 
  
)


  SELECT *
  FROM bamboohr_date 
  