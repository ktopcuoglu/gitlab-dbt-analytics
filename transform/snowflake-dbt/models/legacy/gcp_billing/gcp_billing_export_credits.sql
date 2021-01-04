{{ config({
    "materialized": "incremental",
    "unique_key" : "credit_pk"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('gcp_billing_export_source')}}
    {% if is_incremental() %}

    WHERE uploaded_at >= (SELECT MAX(uploaded_at) FROM {{this}})

    {% endif %}

), renamed as (

    SELECT
        source.primary_key                      AS source_primary_key,
        credits_flat.value['name']::VARCHAR     AS credit_description,
        credits_flat.value['amount']::FLOAT     AS credit_amount,
        source.uploaded_at                      AS uploaded_at,
        {{ dbt_utils.surrogate_key([
            'source_primary_key',
            'credit_description',
            'credits_flat.value'] ) }}          AS credit_pk
    FROM source,
    LATERAL FLATTEN(input => credits) credits_flat

)

SELECT *
FROM renamed
