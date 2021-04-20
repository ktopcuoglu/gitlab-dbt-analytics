{{ config({
        "materialized": "incremental",
        "unique_key": "subscription_snapshot_id",
        "tags": ["incremental_snapshot"]
    })
}}


WITH dim_subscription AS (

    SELECT
      current_date                                                               AS snapshot_date,
      to_number(to_char(current_date,'YYYYMMDD'),'99999999')                     AS snapshot_id,
      *
    FROM {{ ref('dim_subscription') }}

), final AS (

    SELECT {{ dbt_utils.surrogate_key(['snapshot_id', 'dim_subscription_id']) }} AS subscription_snapshot_id,
        *
    FROM dim_subscription

)

SELECT * FROM final