{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('map_merged_crm_account','map_merged_crm_account'),
    ('zuora_contact','zuora_contact_source')
]) }}

, snapshot_dates AS (

   SELECT *
   FROM {{ ref('dim_date') }}
   WHERE date_actual >= '2020-03-01' and date_actual <= CURRENT_DATE

), zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account_snapshots_source') }}
    WHERE is_deleted = FALSE
      AND LOWER(live_batch) != 'batch20'

), zuora_account_spined AS (

    SELECT
      snapshot_dates.date_id AS snapshot_id,
      zuora_account.*
    FROM zuora_account
    INNER JOIN snapshot_dates
      ON snapshot_dates.date_actual >= zuora_account.dbt_valid_from
      AND snapshot_dates.date_actual < {{ coalesce_to_infinity('zuora_account.dbt_valid_to') }}

), joined AS (

    SELECT
      zuora_account_spined.snapshot_id,
      zuora_account_spined.account_id                              AS dim_billing_account_id,
      map_merged_crm_account.dim_crm_account_id,
      zuora_account_spined.account_number                          AS billing_account_number,
      zuora_account_spined.account_name                            AS billing_account_name,
      zuora_account_spined.status                                  AS account_status,
      zuora_account_spined.parent_id,
      zuora_account_spined.sfdc_account_code,
      zuora_account_spined.currency                                AS account_currency,
      zuora_contact.country                                        AS sold_to_country,
      zuora_account_spined.ssp_channel,
      zuora_account_spined.po_required,
      zuora_account_spined.is_deleted,
      zuora_account_spined.batch
    FROM zuora_account_spined
    LEFT JOIN zuora_contact
      ON COALESCE(zuora_account_spined.sold_to_contact_id, zuora_account_spined.bill_to_contact_id) = zuora_contact.contact_id
    LEFT JOIN map_merged_crm_account
      ON zuora_account_spined.crm_id = map_merged_crm_account.sfdc_account_id

), final AS (

    SELECT
        {{ dbt_utils.surrogate_key(['snapshot_id', 'dim_billing_account_id']) }}   AS billing_account_snapshot_id,
        joined.*
    FROM joined

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@jpeguero",
    created_date="2021-08-09",
    updated_date="2021-10-21"
) }}
