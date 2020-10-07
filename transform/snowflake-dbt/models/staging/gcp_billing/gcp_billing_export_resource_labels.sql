{{ config({
    "materialized": "incremental",
    "unique_key" : "resource_label_pk"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('gcp_billing_export_source') }}
    {% if is_incremental() %}

    WHERE uploaded_at >= (SELECT MAX(uploaded_at) FROM {{this}})

    {% endif %}

), renamed as (

    SELECT
        source.primary_key                                      AS source_primary_key,
        resource_labels_flat.value['key']::VARCHAR              AS resource_label_key,
        resource_labels_flat.value['value']::VARCHAR            AS resource_label_value,
        source.uploaded_at                                      AS uploaded_at,
        {{ dbt_utils.surrogate_key([
            'source_primary_key',
            'resource_label_key',
            'resource_label_value'] ) }}                        AS resource_label_pk
    FROM source,
    LATERAL FLATTEN(input=> labels) resource_labels_flat

)

SELECT *
FROM renamed
