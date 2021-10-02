WITH salesforce_date AS (

    {% set tables = ['account', 'campaign', 'campaign_member', 'contact', 'event', 'executive_business_review', 'lead', 'opportunity_stage', 'opportunity_split', 'opportunity_split_type', 'opportunity_team_member', 'opportunity','opportunity_contact_role', 'proof_of_concept', 'record_type', 'statement_of_work', 'task', 'user_role', 'user', 'zqu_quote'] %}					

    {% for table in tables %} 
    SELECT '{{table}}'                                               AS table_name,
        MAX(lastmodifieddate)                                        AS max_date 
    FROM {{source('salesforce', table)}}  
  
  
    {% if not loop.last %}
    UNION ALL
    {% endif %}

{% endfor %} 
  
UNION ALL
  
      {% set tables = [ 'account_history', 'contact_history', 'lead_history', 'opportunity_field_history', 'opportunity_history'] %}					

    {% for table in tables %} 
    SELECT '{{table}}'                                               AS table_name,
        MAX(createddate)                                             AS max_date 
    FROM {{source('salesforce', table)}}  
  
  
    {% if not loop.last %}
    UNION ALL
    {% endif %}

{% endfor %} 
  
)

  SELECT *
  FROM salesforce_date