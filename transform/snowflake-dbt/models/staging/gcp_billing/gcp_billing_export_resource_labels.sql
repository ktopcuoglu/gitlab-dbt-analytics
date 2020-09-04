{{ config({
    "materialized": "view"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('gcp_billing_export_source') }}

)

SELECT
{{ dbt_utils.surrogate_key(['source_primary_key', 'resource_label_key','resource_label_value'] ) }}          AS resource_label_pk,
source.primary_key                                                                                    AS source_primary_key,
labels_flat.value:key::VARCHAR                                                                        AS resource_label_key,
labels_flat.value:value::VARCHAR                                                                      AS resource_label_value
FROM source,
lateral flatten(input=> labels) resource_labels_flat
