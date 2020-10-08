 WITH date_details AS (
    SELECT
      *,
      DENSE_RANK() OVER (ORDER BY first_day_of_fiscal_quarter) AS quarter_number
    FROM {{ ref('date_details') }}
    ORDER BY 1 DESC
), sfdc_opportunity_xf AS (
    SELECT *
    FROM {{ ref('sfdc_opportunity_xf') }}
    WHERE is_deleted = 0

)   SELECT o.*,
        
        CASE WHEN o.order_type_stamped = '1. New - First Order' 
                THEN '1. New'
            WHEN o.order_type_stamped IN ('2. New - Connected', '3. Growth') 
                THEN '2. Growth' 
            WHEN o.order_type_stamped = '4. Churn'
                THEN '3. Churn'
            ELSE '4. Other' END                                                             AS deal_category,
        
        CASE
            WHEN (o.ultimate_parent_sales_segment  = 'Unknown' OR o.ultimate_parent_sales_segment  IS NULL) 
                AND o.user_segment = 'SMB' 
                    THEN 'SMB'
            WHEN (o.ultimate_parent_sales_segment  = 'Unknown' OR o.ultimate_parent_sales_segment  IS NULL) 
                AND o.user_segment = 'Mid-Market' 
                    THEN 'Mid-Market'
            WHEN (o.ultimate_parent_sales_segment  = 'Unknown' OR o.ultimate_parent_sales_segment  IS NULL) 
                AND o.user_segment IN ('Large', 'US West', 'US East', 'Public Sector''EMEA', 'APAC') 
                    THEN 'Large'
            ELSE o.ultimate_parent_sales_segment END                                          AS adj_sales_segment,
        
       -- CASE WHEN o.ultimate_parent_sales_segment IS NULL 
       --         OR o.ultimate_parent_sales_segment = 'Unknown' THEN 'SMB'
       --         ELSE o.ultimate_parent_sales_segment END                                    AS adj_sales_segment,
                
         CASE WHEN o.account_owner_team_stamped in ('APAC', 'MM - APAC')
                THEN 'APAC'
            WHEN o.account_owner_team_stamped in ('MM - EMEA', 'EMEA', 'MM-EMEA')
                THEN 'EMEA'
            WHEN o.account_owner_team_stamped in ('US East', 'MM - East')
                THEN 'US EAST'
            WHEN o.account_owner_team_stamped in ('US West', 'MM - West')
                THEN 'US WEST'
            WHEN o.account_owner_team_stamped in ('Public Sector')
                THEN 'PUBSEC'
            ELSE 'OTHER' END                                                                AS adj_region,

        -- date fields
        d.fiscal_quarter_name_fy                                                            AS close_fiscal_quarter_name,
        dc.fiscal_quarter_name_fy                                                           AS created_fiscal_quarter_name
   FROM sfdc_opportunity_xf o
    -- close date
    INNER JOIN date_details d
        ON d.date_actual = cast(o.close_date as date)
    --created date
    INNER JOIN date_details dc
        ON dc.date_actual = cast(o.created_date as date)
