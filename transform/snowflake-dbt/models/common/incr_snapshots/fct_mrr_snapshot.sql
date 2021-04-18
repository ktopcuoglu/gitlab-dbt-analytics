{{ config({
        "materialized": "incremental",
        "unique_key": "fct_mrr_snapshot_id",
        "tags": ["arr_snapshots"]
    })
}}

/* grain: one record per subscription, product per month */

WITH subscription AS (

    SELECT
      cast(GETDATE() as date)                                       AS snapshot_date,
      to_number(to_char(current_date,'YYYYMMDD'),'99999999')        AS snapshot_id,
      *
    FROM {{ ref('fct_mrr')}}

), final AS (

    SELECT {{ dbt_utils.surrogate_key(['snapshot_id', 'mrr_id']) }} AS fct_mrr_snapshot_id,
        *
    FROM subscription

)

SELECT * FROM final