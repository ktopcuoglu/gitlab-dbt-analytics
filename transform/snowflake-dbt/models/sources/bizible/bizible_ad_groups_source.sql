WITH source AS (

    SELECT *
    FROM {{ source('bizible', 'biz_ad_groups') }}
    ORDER BY uploaded_at DESC

), intermediate AS (

    SELECT 
      d.value as data_by_row, 
      uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), outer => true) d

), renamed AS (

    SELECT DISTINCT
      data_by_row['ad_account_name']::VARCHAR                               AS ad_account_name,
      data_by_row['ad_account_unique_id']::VARCHAR                          AS ad_account_unique_id,
      data_by_row['ad_campaign_name']::VARCHAR                              AS ad_campaign_name,
      data_by_row['ad_campaign_unique_id']::VARCHAR                         AS ad_campaign_unique_id,
      data_by_row['ad_group_name']::VARCHAR                                 AS ad_group_name,
      data_by_row['ad_group_unique_id']::VARCHAR                            AS ad_group_unique_id,
      data_by_row['advertiser_name']::VARCHAR                               AS advertiser_name,
      data_by_row['advertiser_unique_id']::VARCHAR                          AS advertiser_unique_id,
      data_by_row['display_id']::VARCHAR                                    AS display_id,
      data_by_row['entity_type']::VARCHAR                                   AS entity_type,
      data_by_row['first_imported']::VARCHAR                                AS first_imported,
      data_by_row['grouping_key']::VARCHAR                                  AS grouping_key,
      data_by_row['id']::VARCHAR                                            AS id,
      data_by_row['is_active']::VARCHAR                                     AS is_active,
      data_by_row['is_deleted']::VARCHAR                                    AS is_deleted,
      data_by_row['modified_date']::VARCHAR                                 AS modified_date,
      data_by_row['name']::VARCHAR                                          AS name,
      data_by_row['needs_update']::BOOLEAN                                  AS needs_update,
      data_by_row['provider_type']::VARCHAR                                 AS provider_type,
      data_by_row['row_key']::VARCHAR                                       AS row_key,
      data_by_row['url_altenatives']::VARCHAR                               AS url_altenatives,
      data_by_row['url_current']::VARCHAR                                   AS url_current,
      data_by_row['url_old']::VARCHAR                                       AS url_old,
      data_by_row['url_requested']::VARCHAR                                 AS url_requested,
      to_timestamp(data_by_row['_created_date']::INT / 1000)::TIMESTAMP     AS created_date,
      to_timestamp(data_by_row['_deleted_date']::INT / 1000)::TIMESTAMP     AS deleted_date,
      to_timestamp(data_by_row['_modified_date']::INT / 1000)::TIMESTAMP    AS modified_date,
      MAX(uploaded_at)                                                      AS uploaded_at
    FROM intermediate
    {{ dbt_utils.group_by(n=27)}}
      
)

SELECT *
FROM renamed

