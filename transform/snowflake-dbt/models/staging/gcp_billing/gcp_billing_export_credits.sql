{{ config({
    "materialized": "view"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('gcp_billing_export_source') }}

), renamed as (

    SELECT
        {{ dbt_utils.surrogate_key([
            'source.source_surrogate_key',
            'credits_flat.value:name',
            'credits_flat.value'] ) }}              AS credit_pk,
        source.source_surrogate_key                 AS source_surrogate_key,
        credits_flat.value:name::VARCHAR            AS credit_description,
        credits_flat.value:amount::FLOAT            AS credit_amount
    FROM source,
    lateral flatten(input=> credits) credits_flat

)

SELECT * FROM renamed
