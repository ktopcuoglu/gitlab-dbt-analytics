{{ simple_cte([
    ('bamboo_job_current_division_base','bamboo_job_current_division_base'),
    ('sheetload_mapping_sdr_sfdc_bamboohr_source','sheetload_mapping_sdr_sfdc_bamboohr_source'),
    ('dim_crm_user','dim_crm_user'),
    ('dim_date','dim_date')
    
]) }}

, sdr_prep AS (

    SELECT
      employee_id,
      job_role,
      MIN(effective_date) AS start_date,
      MAX(IFNULL(effective_end_date, '2030-12-12')) AS emp_end_date,
      MAX(termination_date) AS termination_date
    FROM bamboo_job_current_division_base
    WHERE LOWER(job_title) LIKE '%sales development representative%' OR LOWER(job_title) LIKE '%sales development team lead%'
    GROUP BY 1, 2

), sdr AS (
  
    SELECT
      sdr_prep.*,
      COALESCE(termination_date, emp_end_date) AS company_or_role_end_date
    FROM sdr_prep

), sdr_ramp AS (
  
    SELECT
      sdr.*,
      sheetload_mapping_sdr_sfdc_bamboohr_source.first_name,
      sheetload_mapping_sdr_sfdc_bamboohr_source.last_name,
      sheetload_mapping_sdr_sfdc_bamboohr_source.active,
      sheetload_mapping_sdr_sfdc_bamboohr_source.user_id AS dim_crm_user_id,
      sheetload_mapping_sdr_sfdc_bamboohr_source.sdr_segment,
      sheetload_mapping_sdr_sfdc_bamboohr_source.sdr_region,
      IFF(sheetload_mapping_sdr_sfdc_bamboohr_source.sdr_region IN ('East', 'West'), 'AMER',
          sheetload_mapping_sdr_sfdc_bamboohr_source.sdr_region) AS sdr_region_grouped,
      IFNULL(sheetload_mapping_sdr_sfdc_bamboohr_source.sdr_order_type, 'Other') AS sdr_order_type,
      CASE
        WHEN DAY(sdr.start_date) < 14 THEN d_1.last_day_of_month
        WHEN DAY(sdr.start_date) >= 14 THEN d_2.last_day_of_month
        ELSE NULL
      END AS sdr_ramp_end_date
    FROM sdr
    INNER JOIN sheetload_mapping_sdr_sfdc_bamboohr_source
      ON sdr.employee_id = sheetload_mapping_sdr_sfdc_bamboohr_source.eeid
    LEFT JOIN dim_crm_user
      ON dim_crm_user.dim_crm_user_id = sheetload_mapping_sdr_sfdc_bamboohr_source.user_id
    LEFT JOIN dim_date AS d_1
      ON DATEADD('month', 1, sdr.start_date) = d_1.date_actual
    LEFT JOIN dim_date AS d_2
      ON DATEADD('month', 2, sdr.start_date) = d_2.date_actual

), dim_date_final AS (
  
    SELECT *
    FROM dim_date
    WHERE first_day_of_month > '2020-11-01'
      AND first_day_of_month <= CURRENT_DATE
  
), final AS (
  
    SELECT
      dim_date.date_actual,
      dim_date.first_day_of_month,
      dim_date.last_day_of_month,
      dim_date.first_day_of_week,
      dim_date.last_day_of_week,
      dim_date.fiscal_quarter_name_fy,
      dim_date.last_day_of_fiscal_quarter,
      CASE
        WHEN dim_date_final.date_actual >= start_date
          AND dim_date_final.date_actual <= sdr_ramp_end_date
          THEN 'Ramping'
        WHEN dim_date_final.date_actual >= start_date
          AND dim_date_final.date_actual > sdr_ramp_end_date
          AND dim_date_final.date_actual <= emp_end_date 
          THEN 'Active'
        ELSE NULL
       END sdr_type,
      sdr_ramp.*
    FROM sdr_ramp
    INNER JOIN dim_date_final
      ON dim_date_final.date_actual BETWEEN sdr_ramp.start_date AND sdr_ramp.company_or_role_end_date
  
)

SELECT *
FROM final
