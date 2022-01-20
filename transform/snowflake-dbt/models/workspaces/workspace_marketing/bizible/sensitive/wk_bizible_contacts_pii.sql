WITH source AS (

    SELECT *
    FROM {{ ref('bizible_contacts_source') }}

)

SELECT *
FROM source