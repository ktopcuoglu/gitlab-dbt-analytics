-- can not define custom database in source.yml source('container_registry','container_registry_download_logs_raw')

{{ config(
    materialized='table'
) }}

WITH source AS (
  SELECT DISTINCT *
  FROM {{ ref('container_registry_download_logs_raw') }}
),

renamed AS (
  SELECT
    correlation_id::VARCHAR AS correlation_id,
    timestamp::TIMESTAMP AS downloaded_at,
    root_repo::VARCHAR AS root_repository,
    vars_name::VARCHAR AS container_path,
    digest::VARCHAR AS container_digest,
    size_bytes::NUMBER AS download_size_bytes,
    remote_ip::VARCHAR AS downloaded_by_ip,
    PARSE_IP(remote_ip, 'INET') AS downloaded_by_parsed_ip,
    downloaded_by_parsed_ip['ipv4']::NUMBER AS downloaded_by_ipv4,
    TO_CHAR(downloaded_by_ipv4, REPEAT('X', LENGTH(downloaded_by_ipv4))) AS downloaded_by_hex_ipv4,
    downloaded_by_parsed_ip['hex_ipv6']::VARCHAR AS downloaded_by_hex_ipv6,
    COALESCE(downloaded_by_hex_ipv4, downloaded_by_hex_ipv6) AS downloaded_by_hex_ip,
    CASE
      WHEN downloaded_by_ipv4 IS NOT NULL THEN 'ip4'
      WHEN downloaded_by_hex_ipv6 IS NOT NULL THEN 'ipv6'
      ELSE 'unknown'
    END AS downloaded_by_ip_type
  FROM source
)

SELECT *
FROM renamed
