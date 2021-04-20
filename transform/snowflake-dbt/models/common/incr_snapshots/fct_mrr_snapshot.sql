{{ config({
        "materialized": "incremental",
        "unique_key": "fct_mrr_snapshot_id",
        "tags": ["incremental_snapshot"]
    })
}}

/* grain: one record per subscription, product per month */

WITH fct_mrr AS (

    SELECT
      cast(GETDATE() as date)                                       AS snapshot_date,
      to_number(to_char(current_date,'YYYYMMDD'),'99999999')        AS snapshot_id,
      *
    FROM {{ ref('fct_mrr')}}

), final AS (

    SELECT {{ dbt_utils.surrogate_key(['snapshot_id', 'mrr_id']) }} AS fct_mrr_snapshot_id,
        *
    FROM fct_mrr

)

SELECT * FROM final