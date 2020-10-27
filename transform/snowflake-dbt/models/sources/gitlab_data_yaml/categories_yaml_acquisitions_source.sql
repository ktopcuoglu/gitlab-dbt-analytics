WITH source AS (

    SELECT *,
      RANK() OVER (PARTITION BY DATE_TRUNC('day', uploaded_at) ORDER BY uploaded_at DESC) AS rank
    FROM {{ source('gitlab_data_yaml', 'categories') }}
    ORDER BY uploaded_at DESC


), stages AS (

    SELECT 
      d.value                                 AS category_object,
      DATE_TRUNC('day', uploaded_at)::DATE    AS snapshot_date,
      rank
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => TRUE) d
  
), acquisitions AS (

    SELECT
      category_object['name']::VARCHAR  AS category_name,
      category_object['stage']::VARCHAR AS category_stage,
      d.value::VARIANT                  AS acquisition_object,
      d.key::VARCHAR                    AS acquisition_key,
      rank,
      snapshot_date
    FROM stages,
    LATERAL FLATTEN(INPUT => parse_json(category_object), OUTER => TRUE) d
    WHERE d.key ILIKE 'acquisition_%'
      

), split_acq_info AS (
  
  -- SPLIT the list of objects about each acquisition into separate rows
  SELECT
    category_name,
    category_stage,
    rank,
    snapshot_date,
    acquisition_key,
    d.value AS info_object,
    d.key AS info_key
  FROM acquisitions a,
  TABLE(FLATTEN(input => acquisition_object, RECURSIVE => TRUE))  d
  WHERE info_key in ('name', 'start_date', 'end_date')
  
), info_combined AS (

  -- Combine back the list of objects about each acquisition into a single object
  SELECT
    category_name,
    category_stage,
    rank,
    snapshot_date,
    acquisition_key,
    OBJECT_AGG(info_key, info_object) acquisition_info
  FROM split_acq_info
  {{ dbt_utils.group_by(n=5) }}
  
), info_parsed AS (

  -- Parse the object about each acquisition
  SELECT
    category_name,
    category_stage,
    rank,
    snapshot_date,
    acquisition_key,
    acquisition_info['name']::VARCHAR AS acquisition_name,
    acquisition_info['start_date']::DATE AS acquisition_start_date,
    acquisition_info['end_date']::DATE AS acquisition_end_date
  FROM info_combined

)

SELECT *
FROM info_parsed