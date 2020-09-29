{{ config({
    "materialized": "view"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('gcp_billing_export_source') }}

), renamed as (

    SELECT

        source.primary_key                                       AS source_primary_key,
        system_labels_flat.value['key']::VARCHAR                 AS system_label_key,
        system_labels_flat.value['value']::VARCHAR               AS system_label_value,
        {{ dbt_utils.surrogate_key([
            'source_primary_key',
            'system_label_key',
            'system_label_value'] ) }}                           AS system_label_pk
    FROM source,
    LATERAL FLATTEN(input=> system_labels) system_labels_flat

)

SELECT *
FROM renamed
