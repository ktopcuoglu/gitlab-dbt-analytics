WITH snapshot_dates AS (
    --Use the 8th calendar day to snapshot ARR, Licensed Users, and Customer Count Metrics
    SELECT DISTINCT
      first_day_of_month,
      snapshot_date_fpa
    FROM {{ ref('dim_date') }}
    ORDER BY 1 DESC

), mart_available_to_renew_snapshot AS (

    SELECT *
    FROM {{ ref('mart_available_to_renew') }}

), final AS (

    SELECT *
    FROM mart_available_to_renew_snapshot
    INNER JOIN snapshot_dates
      ON mart_available_to_renew.term_end_month = snapshot_dates.first_day_of_fiscal_quarter
      AND mart_available_to_renew.snapshot_date = snapshot_dates.snapshot_date_fpa

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2021-11-24",
    updated_date="2021-11-24"
) }}