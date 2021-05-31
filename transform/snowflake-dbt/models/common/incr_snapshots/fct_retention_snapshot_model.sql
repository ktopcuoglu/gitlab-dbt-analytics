{{ config({
        "materialized": "incremental",
        "unique_key": "fct_retention_id",
        "tags": ["edm_snapshot", "retention_snapshots"]
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

),  fct_retention AS (

    SELECT
      *
    FROM {{ ref('fct_retention_snapshot_base') }}

), fct_retention_spined AS (

    SELECT
      snapshot_dates.date_id AS snapshot_id,
      fct_retention.*
    FROM fct_retention
    INNER JOIN snapshot_dates
      ON snapshot_dates.date_actual >= fct_retention.dbt_valid_from
      AND snapshot_dates.date_actual < {{ coalesce_to_infinity('fct_retention.dbt_valid_to') }}

), final AS (

    SELECT
     {{ dbt_utils.surrogate_key(['snapshot_id', 'fct_retention_id']) }} AS fct_retention_snapshot_id,
       *
    FROM fct_retention_spined

)



SELECT * 
FROM final