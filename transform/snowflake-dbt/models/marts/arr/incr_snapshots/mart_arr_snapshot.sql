{{ config({
        "materialized": "incremental",
        "unique_key": "fct_mrr_snapshot_id",
        "tags": ["arr_snapshots"]
    })
}}

WITH mart_arr AS (

    SELECT
      cast(GETDATE() as date) snapshot_date,
      to_number(to_char(current_date,'YYYYMMDD'),'99999999') AS snapshot_id,
      *
    FROM {{ ref('mart_arr') }}

), final AS (

    SELECT {{ dbt_utils.surrogate_key(['snapshot_id', 'primary_key']) }} AS fct_mrr_snapshot_id,
        *
    FROM mart_arr

)

SELECT * FROM final