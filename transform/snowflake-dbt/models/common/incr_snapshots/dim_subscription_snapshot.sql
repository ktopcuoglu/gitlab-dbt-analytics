{{ config({
        "materialized": "incremental",
        "unique_key": "subscription_snapshot_id",
        "tags": ["arr_snapshots"]
    })
}}


WITH subscription AS (

    SELECT
      cast(GETDATE() as date) snapshot_date,
      to_number(to_char(current_date,'YYYYMMDD'),'99999999') AS snapshot_id,
      *
    FROM {{ ref('dim_subscription') }}

), final AS (

    SELECT {{ dbt_utils.surrogate_key(['snapshot_id', 'dim_subscription_id']) }} AS subscription_snapshot_id,
        *
    FROM subscription

)

SELECT * FROM final