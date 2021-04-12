{{ config({
        "materialized": "incremental",
        "unique_key": "subscription_snapshot_id",
        "tags": ["arr_snapshots"]
    })
}}

WITH snapshot_dates AS (

   SELECT *
   FROM {{ ref('dim_date') }}
   WHERE date_actual >= '2020-03-01' and date_actual <= CURRENT_DATE
   {% if is_incremental() %}

   -- this filter will only be applied on an incremental run
   AND date_id > (SELECT max(snapshot_id) FROM {{ this }})

   {% endif %}

), subscription AS (

    SELECT *
    FROM {{ ref('dim_subscription') }}

    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    WHERE snapshot_id > (SELECT max(dim_date.date_id)
                            FROM {{ this }}
                            INNER JOIN dim_date
                            ON dim_date.date_actual = snapshot_date
                            )

    {% endif %}

), zuora_subscription_spined AS (

    SELECT snapshot_dates.date_id AS snapshot_id,
           subscription.*
    FROM subscription
    INNER JOIN snapshot_dates
                        ON snapshot_dates.date_actual >= subscription.subscription_start_date
                            AND snapshot_dates.date_actual <
       {{ coalesce_to_infinity('subscription.subscription_end_date') }}
        QUALIFY rank() OVER (
        PARTITION BY subscription_name
       , snapshot_dates.date_actual
        ORDER BY subscription_start_date DESC) = 1

),   final AS (

  SELECT
    {{ dbt_utils.surrogate_key(['snapshot_id', 'dim_subscription_id']) }},
   *
  FROM zuora_subscription_spined
)

SELECT * FROM final