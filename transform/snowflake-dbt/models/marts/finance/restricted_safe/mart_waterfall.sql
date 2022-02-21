{{config({
    "materialized": "table",
    "transient": false
  })
}}

{{ simple_cte([
    ('rcl','zuora_revenue_revenue_contract_line_source'),
    ('act','zuora_revenue_accounting_type_source'),
    ('rc', 'zuora_revenue_revenue_contract_header_source'),
    ('pob', 'zuora_revenue_revenue_contract_performance_obligation_source'),
    ('rb', 'zuora_revenue_book_source'),
    ('org', 'zuora_revenue_organization_source'),
    ('cal', 'zuora_revenue_calendar_source'),
    ('deleted_schedules', 'zuora_revenue_revenue_contract_schedule_deleted_source'),
    ('zuora_account', 'zuora_account_source'),
    ('zuora_contact_source', 'zuora_contact_source')
]) }}
  
, schd AS (

    SELECT zuora_revenue_revenue_contract_schedule_source.* 
    FROM "PREP".zuora_revenue.zuora_revenue_revenue_contract_schedule_source
    LEFT JOIN deleted_schedules
      ON zuora_revenue_revenue_contract_schedule_source.revenue_contract_schedule_id = deleted_schedules.revenue_contract_schedule_id
    WHERE deleted_schedules.revenue_contract_schedule_id IS NULL

), waterfall_summary AS (

    SELECT
      cal.period_id                                                                                   AS as_of_period_id,
      schd.revenue_contract_schedule_created_period_id,
      schd.revenue_contract_schedule_id,
      schd.revenue_contract_id,
      schd.revenue_contract_line_id,
      schd.root_line_id,
      schd.period_id                                                                                  AS period_id,
      schd.posted_period_id,
      schd.security_attribute_value,
      schd.book_id,
      schd.client_id,
      schd.accounting_segment,
      schd.accounting_type_id,
      schd.is_netting_entry,
      schd.schedule_type,
      schd.amount                                                                                     AS t_at,
      schd.amount * schd.functional_currency_exchange_rate                                            AS f_at,
      (schd.amount * schd.functional_currency_exchange_rate) * schd.reporting_currency_exchange_rate  AS r_at,
      schd.revenue_contract_schedule_created_date,
      schd.revenue_contract_schedule_created_by,
      schd.revenue_contract_schedule_updated_date,
      schd.revenue_contract_schedule_updated_by,
      schd.revenue_contract_schedule_updated_date                                                     AS incremental_update_date
    FROM schd
    INNER JOIN cal
      ON schd.revenue_contract_schedule_created_period_id <= cal.period_id 
        AND schd.period_id >= cal.period_id
    INNER JOIN act
      ON schd.accounting_type_id = act.accounting_type_id
    WHERE act.is_waterfall_account = 'Y'
      AND act.is_cost = 'N'

), waterfall AS (

    SELECT 
      wf.as_of_period_id,
      wf.period_id,
      rb.book_name,
      org.organization_name,
      rc.revenue_contract_id,
      pob.revenue_contract_performance_obligation_name,
      wf.revenue_contract_line_id,
      COALESCE(RCL.customer_name,zuora_account.account_name,RC.customer_name)   AS revenue_contract_customer_name,
      rcl.sales_order_number,
      rcl.sales_order_line_id,
      rcl.customer_number,
      wf.accounting_segment,
      cal.period_name,
      SUM(wf.t_at)                                                              AS amount
     FROM waterfall_summary wf
     INNER JOIN act
       ON wf.accounting_type_id = act.accounting_type_id
     INNER JOIN rcl
       ON wf.root_line_id = rcl.revenue_contract_line_id
     INNER JOIN rc
       ON rcl.revenue_contract_id = rc.revenue_contract_id
         AND rcl.book_id = rc.book_id
     INNER JOIN pob
       ON rcl.revenue_contract_performance_obligation_id = pob.revenue_contract_performance_obligation_id
     INNER JOIN rb
       ON wf.book_id = rb.book_id
     INNER JOIN org
       ON wf.security_attribute_value = org.organization_id
     INNER JOIN cal 
       ON wf.period_id = cal.period_id
     LEFT JOIN zuora_account
       ON rcl.customer_number = zuora_account.account_number
     WHERE act.is_waterfall_account = 'Y'
       AND act.is_cost = 'N'
    {{ dbt_utils.group_by(n=13) }}
  
), rcl_max_prd AS (
   
    SELECT 
      revenue_contract_line_id,
      MAX(period_id)            AS rcl_max_prd_id
    FROM waterfall
    {{ dbt_utils.group_by(n=1) }}
  
), rcl_min_prd AS (
   
    SELECT 
      revenue_contract_line_id,
      MIN(period_id)            AS rcl_min_prd_id
    FROM waterfall
    {{ dbt_utils.group_by(n=1) }}
  
), rc_max_prd AS (

    SELECT 
      revenue_contract_id,
      MAX(period_id)   AS rc_max_prd_id
    FROM waterfall
    GROUP BY 1
  
), last_waterfall_line AS (

    SELECT *
    FROM waterfall
    QUALIFY RANK() OVER (PARTITION BY revenue_contract_line_id ORDER BY as_of_period_id DESC, period_id DESC) = 1
  
), records_to_insert AS ( 

/* 
  Records are inserted based on the last waterfall line available. They will repeat until the last transaction in a revenue contract is fully released.
*/

    SELECT
      cal.period_id                                                     AS as_of_period_id,
      cal.period_id                                                     AS period_id,
      last_waterfall_line.book_name,
      last_waterfall_line.organization_name,
      last_waterfall_line.revenue_contract_id,
      last_waterfall_line.revenue_contract_performance_obligation_name,
      last_waterfall_line.revenue_contract_line_id,
      last_waterfall_line.revenue_contract_customer_name,
      last_waterfall_line.sales_order_number,
      last_waterfall_line.sales_order_line_id,
      last_waterfall_line.customer_number,
      last_waterfall_line.accounting_segment,
      --last_waterfall_line.accounting_type_id,
      last_waterfall_line.period_name,
      0                                                                 AS amount
    FROM last_waterfall_line
    CROSS JOIN cal
    LEFT JOIN rcl_max_prd
      ON last_waterfall_line.revenue_contract_line_id = rcl_max_prd.revenue_contract_line_id
    LEFT JOIN rcl_min_prd
      ON last_waterfall_line.revenue_contract_line_id = rcl_min_prd.revenue_contract_line_id
    LEFT JOIN rc_max_prd
      ON last_waterfall_line.revenue_contract_id = rc_max_prd.revenue_contract_id
    LEFT JOIN waterfall
      ON last_waterfall_line.revenue_contract_line_id = waterfall.revenue_contract_line_id
        AND cal.period_id = waterfall.as_of_period_id
        AND cal.period_id = waterfall.period_id
    WHERE cal.period_id >= rcl_min_prd.rcl_min_prd_id
      AND cal.period_id <= rc_max_prd.rc_max_prd_id
      AND waterfall.revenue_contract_line_id IS NULL
  
), unioned_waterfall AS (

    SELECT *
    FROM waterfall
  
    UNION ALL
  
    SELECT *
    FROM records_to_insert
   
), previous_revenue_base AS (

    SELECT 
      revenue_contract_line_id,
      as_of_period_id,
      accounting_segment,
      period_id, 
      SUM(amount)   AS amount
    FROM unioned_waterfall    AS waterfall
    WHERE as_of_period_id = period_id
    {{ dbt_utils.group_by(n=4) }}
  
), previous_revenue AS (

/*
  To add a column with prior released amounts, this CTE sums the amount released in all periods prior to the current records for each revenue contract line,
  accounting type, accounting segment combination
*/

   SELECT 
     previous_revenue_base.revenue_contract_line_id,
     previous_revenue_base.as_of_period_id,
     previous_revenue_base.period_id,
     previous_revenue_base.amount,
     accounting_segment,
     SUM(amount) OVER (PARTITION BY revenue_contract_line_id, accounting_segment 
                        ORDER BY period_id ASC ROWS BETWEEN unbounded preceding AND 1 preceding
                      )                                                                                 AS previous_total
   FROM previous_revenue_base
   {{ dbt_utils.group_by(n=5) }}

), waterfall_with_previous_revenue AS (

    SELECT 
      unioned_waterfall.*, 
      ZEROIFNULL(previous_revenue.previous_total) AS prior_total
    FROM unioned_waterfall
    LEFT JOIN previous_revenue
      ON unioned_waterfall.revenue_contract_line_id = previous_revenue.revenue_contract_line_id
        AND unioned_waterfall.as_of_period_id = previous_revenue.as_of_period_id
        AND unioned_waterfall.accounting_segment = previous_revenue.accounting_segment
  
), final_waterfall_pivot AS (

    SELECT 
      waterfall_with_previous_revenue.as_of_period_id,
      waterfall_with_previous_revenue.book_name,
      MAX(org.entity_id)                                                            AS entity_id,
      waterfall_with_previous_revenue.organization_name,
      waterfall_with_previous_revenue.revenue_contract_customer_name,
      MAX(rcl.subscription_name)                                                    AS subscription_name,
      {{ get_keyed_nulls('waterfall_with_previous_revenue.sales_order_line_id') }}  AS sales_order_line_id,
      waterfall_with_previous_revenue.revenue_contract_id,
      MAX(rcl.rate_plan_name)                                                       AS rate_plan_name,
      MAX(rcl.rate_plan_charge_name)                                                AS rate_plan_charge_name,
      waterfall_with_previous_revenue.revenue_contract_performance_obligation_name,
      waterfall_with_previous_revenue.accounting_segment,
      MAX(rcl.subscription_start_date)                                              AS subscription_start_date,
      MAX(rcl.revenue_start_date)                                                   AS revenue_start_date,
      MAX(rcl.revenue_end_date)                                                     AS revenue_end_date,
      MAX(rcl.product_family)                                                       AS product_family,
      MAX(rcl.item_number)                                                          AS item_number,
      MAX(zuora_contact_source.country)                                             AS country,
      MAX(rcl.subscription_end_date)                                                AS subscription_end_date,
      waterfall_with_previous_revenue.customer_number,
      MAX(rcl.revenue_contract_line_attribute_16)                                   AS revenue_contract_line_attribute_16,
      {{ dbt_utils.pivot(
                          'period_name', 
                          get_column_values_ordered(
                                                      table = ref('zuora_revenue_calendar_source'),
                                                      column =  'period_name', 
                                                      order_by='SUM(period_id)'
                                                    ),
                          agg = 'SUM',
                          then_value = 'amount',
                          else_value = 0,
                          ) }}
    FROM waterfall_with_previous_revenue
    LEFT JOIN rcl
      ON waterfall_with_previous_revenue.revenue_contract_line_id = rcl.revenue_contract_line_id
    LEFT JOIN org
      ON waterfall_with_previous_revenue.organization_name = org.organization_name
    LEFT JOIN zuora_account
      ON waterfall_with_previous_revenue.customer_number = zuora_account.account_number
    LEFT JOIN zuora_contact_source
      ON COALESCE(zuora_account.sold_to_contact_id, zuora_account.bill_to_contact_id) = zuora_contact_source.contact_id
    GROUP BY 1,2,4,5,7,8,11,12,20

), final_waterfall_with_key AS (

    SELECT 
      {{ dbt_utils.surrogate_key(['CONCAT(as_of_period_id, sales_order_line_id, revenue_contract_id, accounting_segment)']) }} AS primary_key,
      *
    FROM final_waterfall_pivot

)

{{ dbt_audit(
    cte_ref="final_waterfall_with_key",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2021-11-08",
    updated_date="2021-11-18"
) }}