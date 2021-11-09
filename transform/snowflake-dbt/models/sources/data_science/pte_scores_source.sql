WITH source AS (

    SELECT *
    FROM {{ source('data_science', 'pte_scores') }}

), intermediate AS (

    SELECT
      d.value as data_by_row,
      uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext), outer => true) d

), parsed AS (

    SELECT
      data_by_row['crm_account_id']::VARCHAR                AS crm_account_id,
      data_by_row['decile']::INT                            AS decile,
      data_by_row['group']::INT                             AS score_group,
      data_by_row['importance']::INT                        AS importance,
      data_by_row['score']::NUMBER                          AS score,
      uploaded_at::TIMESTAMP                                AS uploaded_at

    FROM intermediate

)
SELECT * 
FROM parsed
