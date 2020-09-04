WITH source AS (

    SELECT *
    FROM {{ ref('gcp_billing_export_source') }}

)

SELECT
{{ dbt_utils.surrogate_key(['primary_key', 'credit_description','credit_name'] ) }} AS credits_pk,
source.primary_key                                                                  AS source_primary_key,
credits_flat.value:name::VARCHAR                                                    AS credit_description,
credits_flat.value:amount::FLOAT                                                    AS credit_amount,
FROM source,
lateral flatten(input=> credits) credits_flat
