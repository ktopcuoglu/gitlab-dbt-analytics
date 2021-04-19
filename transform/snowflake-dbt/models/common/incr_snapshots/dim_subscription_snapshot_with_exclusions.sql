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

), dim_subscription AS (

    SELECT
      *
    FROM {{ ref('dim_subscription') }}

), subscription_valid_dates AS (

    SELECT DISTINCT subscription_id, MAX(dbt_valid_from) dbt_valid_from, MAX(dbt_valid_to) dbt_valid_to
    FROM {{ ref('zuora_subscription_snapshots_source') }}
    WHERE subscription_status NOT IN ('Draft', 'Expired')
      AND is_deleted = FALSE
      AND exclude_from_analysis IN ('False', '')
    GROUP BY 1

), dim_subscription_spined AS (

    SELECT
      snapshot_dates.date_id AS snapshot_id,
      dim_subscription.*
    FROM dim_subscription
    INNER JOIN subscription_valid_dates
      ON subscription_valid_dates.subscription_id = dim_subscription.dim_subscription_id
    INNER JOIN snapshot_dates
      ON snapshot_dates.date_actual >= subscription_valid_dates.dbt_valid_from
      AND snapshot_dates.date_actual < {{ coalesce_to_infinity('subscription_valid_dates.dbt_valid_to') }}

), final AS (

    SELECT
      {{ dbt_utils.surrogate_key(['snapshot_id', 'dim_subscription_id']) }} AS subscription_snapshot_id,
      cast(GETDATE() as date) snapshot_date,
      *
    FROM dim_subscription_spined

)

SELECT * FROM final