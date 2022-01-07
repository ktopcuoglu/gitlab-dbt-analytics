WITH source AS (

    SELECT *
    FROM {{ source('zuora_central_sandbox', 'order_tcb') }}

), renamed AS(

    SELECT 
      id                                    AS  id,
      created_by_id                         AS  created_by_id,
      created_date                          AS  created_date,
      discount_charge_id                    AS  discount_charge_id,
      end_date                              AS  end_date,
      invoice_owner_id                      AS  invoice_owner_id,
      start_date                            AS  start_date,
      subscription_owner_id                 AS  subscription_owner_id,
      tax                                   AS  tax,
      term                                  AS  term,
      type                                  AS  type,
      updated_by_id                         AS  updated_by_id,
      updated_date                          AS  updated_date,
      value                                 AS  value,
      _fivetran_deleted                     AS  _fivetran_deleted,
      sold_to_contact_id                    AS  sold_to_contact_id,
      account_id                            AS  account_id,
      order_action_id                       AS  order_action_id,
      product_id                            AS  product_id,
      subscription_version_amendment_id     AS  subscription_version_amendment_id,
      subscription_id                       AS  subscription_id,
      default_payment_method_id             AS  default_payment_method_id,
      rate_plan_charge_id                   AS  rate_plan_charge_id,
      product_rate_plan_id                  AS  product_rate_plan_id,
      bill_to_contact_id                    AS  bill_to_contact_id,
      order_id                              AS  order_id,
      rate_plan_id                          AS  rate_plan_id,
      product_rate_plan_charge_id           AS  product_rate_plan_charge_id,
      _fivetran_synced                      AS  _fivetran_synced,
      amendment_id                          AS  amendment_id

    FROM source

)

SELECT *
FROM renamed
