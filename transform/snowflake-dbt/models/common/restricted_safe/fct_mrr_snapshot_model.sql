{{ config({
        "materialized": "incremental",
        "unique_key": "fct_mrr_snapshot_id",
        "tags": ["edm_snapshot", "arr_snapshots"]
    })
}}

/* grain: one record per subscription, product per month */
WITH snapshot_dates AS (

   SELECT *
   FROM {{ ref('dim_date') }}
   WHERE date_actual >= '2020-03-01' and date_actual <= CURRENT_DATE
   {% if is_incremental() %}

   -- this filter will only be applied on an incremental run
   AND date_id > (SELECT max(snapshot_id) FROM {{ this }})

   {% endif %}

),  fct_mrr AS (

    SELECT
      *
    FROM {{ ref('prep_fct_mrr_snapshot_base') }}

), prep_charge AS (

    SELECT
      prep_charge.*,
      charge_created_date   AS valid_from,
      '9999-12-31'          AS valid_to
    FROM {{ ref('prep_charge') }}
    WHERE rate_plan_charge_name = 'manual true up allocation'

), manual_charges AS (

    SELECT
      date_id                                 AS snapshot_id,
      {{ dbt_utils.surrogate_key(['date_id', 'subscription_name', 'dim_product_detail_id', 'mrr']) }}
          AS mrr_id,
      date_id                                 AS dim_date_id,
      dim_charge_id                           AS dim_charge_id,
      dim_product_detail_id                   AS dim_product_detail_id,
      dim_subscription_id                     AS dim_subscription_id,
      dim_billing_account_id                  AS dim_billing_account_id,
      dim_crm_account_id                      AS dim_crm_account_id,
      mrr                                     AS mrr,
      arr                                     AS arr,
      quantity                                AS quantity,
      ARRAY_AGG(unit_of_measure)              AS unit_of_measure,
      NULL                                    AS created_by,
      NULL                                    AS updated_by,
      NULL                                    AS model_created_date,
      NULL                                    AS model_updated_date,
      NULL                                    AS dbt_created_at,
      NULL                                    AS dbt_scd_id,
      NULL                                    AS dbt_updated_at,
      valid_from                              AS dbt_valid_from,
      '9999-12-31'                            AS dbt_valid_to,
      subscription_status
    FROM prep_charge
    INNER JOIN snapshot_dates
      ON snapshot_dates.date_actual >= prep_charge.valid_from
      AND snapshot_dates.date_actual < COALESCE( prep_charge.valid_to, '9999-12-31'::TIMESTAMP)
    -- NOTE THE GAP IN THE GROUPINGS BELOW,
    -- We need to group by everything except for unit of measure.
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,13,14,15,16,17,18,19,20,21,22

), non_manual_charges AS (

    SELECT
      snapshot_dates.date_id AS snapshot_id,
      fct_mrr.*
    FROM fct_mrr
    INNER JOIN snapshot_dates
      ON snapshot_dates.date_actual >= fct_mrr.dbt_valid_from
      AND snapshot_dates.date_actual < {{ coalesce_to_infinity('fct_mrr.dbt_valid_to') }}

), combined_charges AS (

    SELECT *
    FROM manual_charges

    UNION ALL

    SELECT *
    FROM non_manual_charges

), final AS (

    SELECT
     {{ dbt_utils.surrogate_key(['snapshot_id', 'mrr_id']) }} AS fct_mrr_snapshot_id,
       *
    FROM combined_charges

)



SELECT * 
FROM final