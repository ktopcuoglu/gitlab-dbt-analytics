{{ config({
        "materialized": "incremental",
        "unique_key": "fct_mrr_snapshot_id",
        "tags": ["edm_snapshot", "fct_mrr_snapshot"]
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
    FROM {{ ref('fct_mrr_snapshot_base') }}

), fct_mrr_spined AS (

    SELECT
      snapshot_dates.date_id AS snapshot_id,
      fct_mrr.*
    FROM fct_mrr
    INNER JOIN snapshot_dates
      ON snapshot_dates.date_actual >= fct_mrr.dbt_valid_from
      AND snapshot_dates.date_actual < {{ coalesce_to_infinity('fct_mrr.dbt_valid_to') }}

), final AS (

    SELECT
     {{ dbt_utils.surrogate_key(['snapshot_id', 'mrr_id']) }} AS fct_mrr_snapshot_id,
       *
    FROM fct_mrr_spined

)



SELECT * 
FROM final