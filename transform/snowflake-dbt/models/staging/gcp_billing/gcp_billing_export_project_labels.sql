{{ config({
    "materialized": "view"
    })
}}
WITH source AS (

    SELECT *
    FROM {{ ref('gcp_billing_export_source') }}

), renamed as (

    SELECT
        source.source_surrogate_key                              AS source_surrogate_key,
        project_labels_flat.value['key']::VARCHAR                AS project_label_key,
        project_labels_flat.value['value']::VARCHAR              AS project_label_value,
        {{ dbt_utils.surrogate_key([
            'source_surrogate_key',
            'project_label_key',
            'project_label_value'] ) }}                          AS project_label_pk
    FROM source,
    LATERAL FLATTEN(input=> project_labels) project_labels_flat
)

SELECT * 
FROM renamed
