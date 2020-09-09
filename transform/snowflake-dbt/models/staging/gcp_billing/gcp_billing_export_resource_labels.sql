{{ config({
    "materialized": "view"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('gcp_billing_export_source') }}

), renamed as (

    SELECT
        {{ dbt_utils.surrogate_key(['source.primary_key', 'resource_labels_flat.value:key','resource_labels_flat.value:value'] ) }}   AS resource_label_pk,
        source.primary_key                                                                                                            AS source_primary_key,
        resource_labels_flat.value:key::VARCHAR                                                                                       AS resource_label_key,
        resource_labels_flat.value:value::VARCHAR                                                                                     AS resource_label_value
    FROM source,
    lateral flatten(input=> labels) resource_labels_flat
)

SELECT * FROM renamed
