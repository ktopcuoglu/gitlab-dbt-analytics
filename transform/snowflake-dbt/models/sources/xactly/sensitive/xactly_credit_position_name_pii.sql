WITH source AS (

    SELECT *
    FROM {{ ref('xactly_credit_source') }}

), position_name_pii AS (

    SELECT

      participant_id,
      {{ nohash_sensitive_columns('xactly_credit_source', 'position_name') }}

    FROM source
    
)

SELECT *
FROM position_name_pii