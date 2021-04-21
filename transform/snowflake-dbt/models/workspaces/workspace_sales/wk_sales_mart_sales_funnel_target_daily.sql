  
  {{ config(alias='mart_sales_funnel_target_daily') }}


  WITH date_details AS (
    
    SELECT *
    FROM {{ ref('wk_sales_date_details') }}  

  ), mart_sales_funnel_target AS (

    SELECT *
    FROM {{ref('mart_sales_funnel_target_daily')}} 
  
  ), final AS (

    SELECT
          funnel_target.*,
          target_date.fiscal_quarter_name_fy           AS target_fiscal_quarter_name,
          target_date.first_day_of_fiscal_quarter      AS target_fiscal_quarter_date, 
          target_date.day_of_fiscal_quarter_normalised AS target_day_of_fiscal_quarter_normalised,  

          COALESCE(funnel_target.sales_qualified_source_name,'NA')                                      AS sales_qualified_source,
          COALESCE(CONCAT(funnel_target.crm_user_sales_segment,'_',funnel_target.crm_user_region),'NA') AS sales_team_rd_asm_level,
          COALESCE(funnel_target.crm_user_sales_segment,'NA')                                           AS sales_team_cro_level,

          CASE 
            WHEN funnel_target.order_type_name = '3. Growth' 
                THEN '2. Growth'
            WHEN funnel_target.order_type_name = '1. New - First Order' 
                THEN '1. New'
              ELSE '3. Other'
          END                                                AS deal_group
    FROM mart_sales_funnel_target funnel_target
      INNER JOIN  date_details target_date
      ON target_date.date_actual = funnel_target.target_date
    WHERE LOWER(deal_group) LIKE ANY ('%growth%','%new%')
  )

  SELECT *
  FROM final