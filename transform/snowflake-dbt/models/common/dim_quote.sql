{{ config({
        "schema": "common"
    })
}}

WITH sfdc_quote_source AS (

    SELECT *
    FROM {{ ref('sfdc_quote_source') }}
    WHERE is_deleted = FALSE

), final AS (

    SELECT
      quote_id                                         AS dim_quote_id,
      quote_name,
      quote_status,
      is_primary_quote,
      date_trunc('month', opps.closedate::DATE)        AS quote_close_month,
      quote.zqu__startdate__c::DATE                    AS quote_start_date
    FROM sfdc_quote_source

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2020-12-28",
    updated_date="2020-12-28"
) }}