WITH source AS (

    SELECT *
    FROM {{ ref('zengrc_audit_source') }}

)

SELECT *
FROM source