WITH source AS (

    SELECT *
    FROM {{ source('zendesk', 'organizations') }}

),

renamed AS (

    SELECT

        --ids
        id                                                  AS organization_id,
        organization_fields__salesforce_id::VARCHAR         AS sfdc_account_id,

        --fields
        name                                                AS organization_name,
        tags                                                AS organization_tags,
        organization_fields__aar::NUMBER                    AS arr,
        organization_fields__market_segment::VARCHAR        AS organization_market_segment,

        --dates
        created_at,
        updated_at

    FROM source

)

SELECT *
FROM renamed
