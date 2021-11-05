WITH zuora_date AS (

    {% set tables = ['account' ,'accounting_period' ,'amendment' ,'contact' ,'discount_applied_metrics' ,'invoice_item' ,'invoice','invoice_payment' ,'product' ,'product_rate_plan' ,'product_rate_plan_charge' ,'product_rate_plan_charge_tier' ,'rate_plan' ,'rate_plan_charge' ,'rate_plan_charge_tier' ,'refund' ,'revenue_schedule_item' ,'subscription' ] %}					

    {% for table in tables %} 
    SELECT '{{table}}'                                              AS table_name,
        MAX(CREATEDDATE)                                            AS max_date 
    FROM {{source('zuora', table)}}  
  
  
    {% if not loop.last %}
    UNION ALL
    {% endif %}

{% endfor %} 
  
)


  SELECT *
  FROM zuora_date