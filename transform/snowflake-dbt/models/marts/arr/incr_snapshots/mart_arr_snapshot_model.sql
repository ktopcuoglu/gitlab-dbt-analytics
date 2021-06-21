{{ config({
        "materialized": "incremental",
        "unique_key": "mart_arr_snapshot_id",
        "tags": ["edm_snapshot", "arr_snapshots"],
        "schema": "common_mart_sales"
    })
}}

/* grain: one record per subscription, product per month */
WITH snapshot_dates AS (

    SELECT *
    FROM {{ ref('dim_date') }}
    WHERE date_actual >= '2020-03-01'
    AND date_actual <= CURRENT_DATE {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    AND date_id > (SELECT max(snapshot_id) FROM {{ this }})

    {% endif %}

), mart_arr AS (

    SELECT *
    FROM {{ ref('mart_arr_snapshot_base') }}

), prep_charge AS (

    SELECT
      prep_charge.*,
      charge_created_date AS valid_from,
      '9999-12-31'        AS valid_to
    FROM {{ ref('prep_charge') }}
    WHERE rate_plan_charge_name = 'manual true up allocation'

), manual_charges AS (

    SELECT
      snapshot_dates.date_id                 AS snapshot_id,
      snapshot_dates.date_actual             AS snapshot_date,
      {{ dbt_utils.surrogate_key(['dim_date.date_actual', 'subscription_name', 'dim_product_detail_id']) }} AS primary_key,
      snapshot_dates.date_actual            AS arr_month,
      NULL                                  AS fiscal_quarter_name_fy,
      NULL                                  AS fiscal_year,
      NULL                                  AS subscription_start_month,
      NULL                                  AS subscription_end_month,
      dim_billing_account_id                AS dim_billing_account_id,
      NULL                                  AS sold_to_country,
      NULL                                  AS billing_account_name,
      NULL                                  AS billing_account_number,
      dim_crm_account_id                    AS dim_crm_account_id,
      NULL                                  AS crm_account_name,
      dim_parent_crm_account_id             AS dim_parent_crm_account_id,
      NULL                                  AS parent_crm_account_name,
      NULL                                  AS parent_crm_account_billing_country,
      NULL                                  AS parent_crm_account_sales_segment,
      NULL                                  AS parent_crm_account_industry,
      NULL                                  AS parent_crm_account_owner_team,
      NULL                                  AS parent_crm_account_sales_territory,
      NULL                                  AS parent_crm_account_tsp_region,
      NULL                                  AS parent_crm_account_tsp_sub_region,
      NULL                                  AS parent_crm_account_tsp_area,
      NULL                                  AS crm_account_tsp_region,
      NULL                                  AS crm_account_tsp_sub_region,
      NULL                                  AS crm_account_tsp_area,
      NULL                                  AS health_score,
      NULL                                  AS health_score_color,
      NULL                                  AS health_number,
      subscription_status                   AS subscription_status,
      NULL                                  AS subscription_sales_type,
      subscription_name                     AS subscription_name,
      NULL                                  AS subscription_name_slugify,
      NULL                                  AS oldest_subscription_in_cohort,
      NULL                                  AS subscription_lineage,
      NULL                                  AS subscription_cohort_month,
      NULL                                  AS subscription_cohort_quarter,
      NULL                                  AS billing_account_cohort_month,
      NULL                                  AS billing_account_cohort_quarter,
      NULL                                  AS crm_account_cohort_month,
      NULL                                  AS crm_account_cohort_quarter,
      NULL                                  AS parent_account_cohort_month,
      NULL                                  AS parent_account_cohort_quarter,
      NULL                                  AS product_tier_name,
      NULL                                  AS product_delivery_type,
      NULL                                  AS service_type,
      NULL                                  AS product_rate_plan_name,
      ARRAY_AGG(unit_of_measure)              AS unit_of_measure,
      mrr                                   AS mrr,
      arr                                   AS arr,
      quantity                              AS quantity,
      NULL                                  AS months_since_billing_account_cohort_start,
      NULL                                  AS quarters_since_billing_account_cohort_start,
      NULL                                  AS months_since_crm_account_cohort_start,
      NULL                                  AS quarters_since_crm_account_cohort_start,
      NULL                                  AS months_since_parent_account_cohort_start,
      NULL                                  AS quarters_since_parent_account_cohort_start,
      NULL                                  AS months_since_subscription_cohort_start,
      NULL                                  AS quarters_since_subscription_cohort_start,
      prep_charge.created_by                AS created_by,
      prep_charge.updated_by                AS updated_by,
      prep_charge.model_created_date        AS model_created_date,
      prep_charge.model_updated_date        AS model_updated_date,
      prep_charge.dbt_created_at            AS dbt_created_at,
      NULL                                  AS dbt_scd_id,
      prep_charge.dbt_updated_at            AS dbt_updated_at,
      valid_from                            AS dbt_valid_from,
      valid_to                              AS dbt_valid_to,
      NULL                                  AS is_jihu_account,
      dim_subscription_id                   AS dim_subscription_id,
      NULL                                  AS dim_subscription_id_original
    FROM prep_charge
    INNER JOIN snapshot_dates
    ON snapshot_dates.date_actual >= prep_charge.valid_from
    AND snapshot_dates.date_actual < COALESCE(prep_charge.valid_to, '9999-12-31'::TIMESTAMP)
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,

), non_manual_charges AS (

    SELECT
      snapshot_dates.date_id     AS snapshot_id,
      snapshot_dates.date_actual AS snapshot_date,
      mart_arr.*
    FROM mart_arr
    INNER JOIN snapshot_dates
    ON snapshot_dates.date_actual >= mart_arr.dbt_valid_from
    AND snapshot_dates.date_actual < {{ coalesce_to_infinity('mart_arr.dbt_valid_to') }}

), combined_charges AS (

    SELECT *
    FROM manual_charges

    UNION ALL

    SELECT *
    FROM non_manual_charges

 ), final AS (

     SELECT
       {{ dbt_utils.surrogate_key(['snapshot_id', 'primary_key']) }} AS mart_arr_snapshot_id,
       *
     FROM combined_charges

)


SELECT *
FROM final