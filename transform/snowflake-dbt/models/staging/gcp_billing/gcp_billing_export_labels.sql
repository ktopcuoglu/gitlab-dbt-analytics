WITH source AS (

    SELECT *
    FROM {{ ref('gcp_billing_export_source') }}

)

SELECT
{{ dbt_utils.surrogate_key(['primary_key', 'label_key','label_value'] ) }}          AS label_pk,
source.primary_key                                                                  AS source_primary_key,
labels_flat.value:key::VARCHAR                                                      AS label_key,
labels_flat.value:value::VARCHAR                                                    AS label_value,
FROM source,
lateral flatten(input=> labels) labels_flat
