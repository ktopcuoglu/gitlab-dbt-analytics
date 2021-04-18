{{ config({
        "materialized": "incremental",
        "unique_key": "fct_mrr_snapshot_id",
        "tags": ["arr_snapshots"]
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

), fct_mrr AS (

    SELECT *
    FROM {{ ref('fct_mrr') }}

    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    WHERE dim_date_id > (SELECT max(snapshot_dates.date_id)
                            FROM {{ this }}
                            INNER JOIN snapshot_dates
                            ON snapshot_dates.date_id = snapshot_id
                            )

    {% endif %}

), fct_mrr_spined AS (

    SELECT snapshot_dates.date_id AS snapshot_id,
           fct_mrr.*
    FROM fct_mrr
    INNER JOIN snapshot_dates
                        ON snapshot_dates.date_id >= fct_mrr.dim_date_id
                            AND snapshot_dates.date_id < fct_mrr.dim_date_id
        QUALIFY rank() OVER (
        PARTITION BY dim_subscription_id
       , snapshot_dates.date_actual
        ORDER BY dim_date_id DESC) = 1

),   final AS (

  SELECT
    {{ dbt_utils.surrogate_key(['snapshot_id', 'mrr_id']) }} AS fct_mrr_snapshot_id,
    cast(GETDATE() as date) snapshot_date,
   *
  FROM fct_mrr_spined
)

SELECT * FROM final
