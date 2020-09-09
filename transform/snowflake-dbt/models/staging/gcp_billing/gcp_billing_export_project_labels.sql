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
            'source.primary_key', 
            'project_labels_flat.value:key',
            'project_labels_flat.value:value'] ) }}              AS project_label_pk,
        source.primary_key                                       AS source_primary_key,
        project_labels_flat.value:key::VARCHAR                   AS project_label_key,
        project_labels_flat.value:value::VARCHAR                 AS project_label_value
    FROM source,
    lateral flatten(input=> project_labels) project_labels_flat
)

SELECT * FROM renamed
