{{ config({
        "materialized": "incremental",
        "unique_key": "primary_key",
        "tags": ["arr_snapshots"]
    })
}}

WITH snapshot_dates AS (

   SELECT *
   FROM {{ ref('dim_date') }}
   WHERE date_actual >= '2020-03-01' and date_actual <= CURRENT_DATE
   {% if is_incremental() %}

   -- this filter will only be applied on an incremental run
   AND date_id > (SELECT max(mart_arr_snapshot_id) FROM {{ this }})

   {% endif %}

), mart_arr AS (

    SELECT *
    FROM {{ ref('mart_arr') }}

    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    WHERE arr_month > (SELECT max(snapshot_dates.date_actual)
                            FROM {{ this }}
                            INNER JOIN snapshot_dates
                            ON snapshot_dates.date_actual = snapshot_date
                            )

    {% endif %}

), mart_arr_spined AS (

    SELECT snapshot_dates.date_id AS snapshot_id,
           mart_arr.*
    FROM mart_arr
    INNER JOIN snapshot_dates
                        ON snapshot_dates.date_actual >= mart_arr.arr_month
                            AND snapshot_dates.date_actual <
       {{ coalesce_to_infinity('mart_arr.arr_month') }}
        QUALIFY rank() OVER (
        PARTITION BY subscription_name
       , snapshot_dates.date_actual
        ORDER BY arr_month DESC) = 1

),  final AS (

  SELECT
    {{ dbt_utils.surrogate_key(['snapshot_id', 'primary_key']) }} as mart_arr_snapshot_id,
     cast(GETDATE() as date) snapshot_date,
   *
  FROM mart_arr_spined
)

SELECT *
FROM final