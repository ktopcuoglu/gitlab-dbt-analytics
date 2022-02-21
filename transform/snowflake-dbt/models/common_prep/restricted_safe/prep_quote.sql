WITH sfdc_zqu_quote_source AS (

    SELECT *
    FROM {{ ref('sfdc_zqu_quote_source') }}
    WHERE is_deleted = FALSE

), final AS (

    SELECT
      quote_id                                      AS dim_quote_id,
      zqu__number                                   AS quote_number,
      zqu_quote_name                                AS quote_name,
      zqu__status                                   AS quote_status,
      zqu__primary                                  AS is_primary_quote,
      zqu__start_date                               AS quote_start_date
    FROM sfdc_zqu_quote_source

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2021-01-07",
    updated_date="2021-01-07"
) }}