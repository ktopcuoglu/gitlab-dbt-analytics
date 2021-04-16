  
  {{ config(alias='mart_sales_funnel_target') }}

  SELECT *,
        sales_qualified_source_name                         AS sales_qualified_source,
        CONCAT(crm_user_sales_segment,'_',crm_user_region)  AS sales_team_rd_asm_level,
        crm_user_sales_segment                              AS sales_team_cro_level,
          CASE 
          WHEN order_type_name = '3. Growth' 
              THEN '2. Growth'
          WHEN order_type_name = '1. New - First Order' 
              THEN '1. New'
            ELSE '3. Other'
         END                                                AS deal_group
  FROM {{ref('mart_sales_funnel_target')}}   
  WHERE LOWER(deal_group) LIKE ANY ('%growth%','%new%')