{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('pi_targets', 'performance_indicators_yaml_historical'),
    ('dim_date', 'dim_date'),
    ])

}},

first_day_of_month AS (

  SELECT DISTINCT first_day_of_month AS reporting_month
  FROM dim_date

),

most_recent_yml_record AS (
  -- just grabs the most recent record of each metrics_path that dont have a null estimated target
  SELECT *
  FROM pi_targets
  WHERE pi_monthly_estimated_targets IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY pi_metric_name ORDER BY snapshot_date DESC) = 1

),

flattened_monthly_targets AS (
  -- flatten the json record from the yml file to get the target value and end month for each key:value pair
  SELECT
    pi_metric_name,
    d.value,
    PARSE_JSON(d.path)[0]::TIMESTAMP AS target_end_month
  FROM most_recent_yml_record,
    LATERAL FLATTEN(INPUT => PARSE_JSON(pi_monthly_estimated_targets), OUTER => TRUE) AS d

),

monthly_targets_with_intervals AS (
  -- Calculate the reporting intervals for the pi_metric_name. Each row will have a start and end date
  SELECT
    *,
    -- check if the row above the current row has a target_end_date:
    -- TRUE: then the start month = target_end_date from previous ROW_NUMBER
    -- FALSE: then make the start_month a year ago from TODAY
    COALESCE(
      LAG(target_end_month) OVER (PARTITION BY 1 ORDER BY target_end_month),
      DATEADD('month', -12, CURRENT_DATE)
 ) AS target_start_month
  FROM flattened_monthly_targets

),

final_targets AS (
  -- join each metric_name and value to the reporting_month it corresponds WITH
  -- join IF reporting_month greater than metric start_month and reporting_month less than or equal to the target end month/ CURRENT_DATE
  SELECT
    {{ dbt_utils.surrogate_key(['reporting_month', 'pi_metric_name']) }} AS fct_performance_indicator_targets_id,
    reporting_month,
    pi_metric_name,
    value AS target_value
  FROM first_day_of_month
  INNER JOIN monthly_targets_with_intervals
  WHERE reporting_month > target_start_month
    AND reporting_month <= COALESCE(target_end_month, CURRENT_DATE)

),

results AS (

  SELECT *
  FROM final_targets

)


{{ dbt_audit(
    cte_ref="results",
    created_by="@dihle",
    updated_by="@iweeks",
    created_date="2022-04-20",
    updated_date="2022-05-12"
) }}
