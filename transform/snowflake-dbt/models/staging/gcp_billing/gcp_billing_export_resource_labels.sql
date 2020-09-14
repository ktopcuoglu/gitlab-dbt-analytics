{{ config({
    "materialized": "view"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('gcp_billing_export_source') }}

), renamed as (

    SELECT
        source.source_surrogate_key                             AS source_surrogate_key,
        resource_labels_flat.value['key']::VARCHAR              AS resource_label_key,
        resource_labels_flat.value['value']::VARCHAR            AS resource_label_value,
        {{ dbt_utils.surrogate_key([
            'source_surrogate_key',
            'resource_label_key',
            'resource_label_value'] ) }}                        AS resource_label_pk
    FROM source,
    LATERAL FLATTEN(input=> labels) resource_labels_flat

)

SELECT *
FROM renamed
