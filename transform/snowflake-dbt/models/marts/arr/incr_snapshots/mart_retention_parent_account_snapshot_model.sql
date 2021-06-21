{{ config({
        "materialized": "incremental",
        "unique_key": "mart_retention_parent_account_snapshot_id",
        "tags": ["edm_snapshot", "arr_snapshots"],
        "schema": "common"
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

), dim_crm_account AS (

    SELECT *
    FROM {{ ref('dim_crm_account') }}

), mart_retention_parent_account AS (

    SELECT *
    FROM {{ ref('mart_retention_parent_account_snapshot_base') }}

), filtered_retention AS (

    SELECT
      mart_retention_parent_account.*
    FROM mart_retention_parent_account
    JOIN dim_crm_account
      ON dim_crm_account.dim_parent_crm_account_ID = mart_retention_parent_account.dim_crm_account_id
    WHERE is_jihu_account = FALSE

), mart_retention_parent_account_spined AS (

    SELECT
      snapshot_dates.date_id AS snapshot_id,
      filtered_retention.*
    FROM filtered_retention
    INNER JOIN snapshot_dates
      ON snapshot_dates.date_actual >= filtered_retention.dbt_valid_from
      AND snapshot_dates.date_actual < {{ coalesce_to_infinity('filtered_retention.dbt_valid_to') }}

), final AS (

    SELECT
     {{ dbt_utils.surrogate_key(['snapshot_id', 'primary_key']) }} AS mart_retention_parent_account_snapshot_id,
       *
    FROM mart_retention_parent_account_spined

)


SELECT * 
FROM final