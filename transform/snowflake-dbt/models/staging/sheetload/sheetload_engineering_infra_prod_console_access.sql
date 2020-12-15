WITH source AS (

    SELECT *
    FROM {{ ref('engineering_infra_prod_console_access_source') }}

)

SELECT *
FROM source

