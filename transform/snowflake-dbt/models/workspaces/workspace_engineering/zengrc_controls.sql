WITH source AS (

    SELECT *
    FROM {{ ref('zengrc_control_source') }}

)

SELECT *
FROM source