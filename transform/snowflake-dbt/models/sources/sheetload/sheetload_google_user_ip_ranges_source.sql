WITH source AS (

  SELECT *
  FROM {{ source('sheetload','google_user_ip_ranges') }}

),

renamed AS (
  SELECT
    meta.value['creationTime']::TIMESTAMP AS creation_at,
    prefixes.value['ipv4Prefix']::VARCHAR AS ip_v4_prefix,
    prefixes.value['ipv6Prefix']::VARCHAR AS ip_v6_prefix,
    PARSE_IP(ip_v4_prefix, 'INET') AS parsed_ipv4,
    PARSE_IP(ip_v6_prefix, 'INET') AS parsed_ipv6,
    COALESCE(parsed_ipv4,parsed_ipv6) AS parsed_ip,
    PARSE_IP(ip_v4_prefix, 'INET')['ipv4']::NUMBER AS ipv4,
    TO_CHAR(ipv4, REPEAT('X', LENGTH(ipv4))) AS hex_ipv4,
    PARSE_IP(ip_v4_prefix, 'INET')['ipv4_range_end']::NUMBER AS ipv4_range_end,
    PARSE_IP(ip_v4_prefix, 'INET')['ipv4_range_start']::NUMBER AS ipv4_range_start,
    TO_CHAR(ipv4_range_start, REPEAT('X', LENGTH(ipv4_range_start))) AS hex_ipv4_range_start,
    TO_CHAR(ipv4_range_end, REPEAT('X', LENGTH(ipv4_range_end))) AS hex_ipv4_range_end,    
    PARSE_IP(ip_v6_prefix, 'INET')['hex_ipv6']::VARCHAR AS hex_ipv6,
    PARSE_IP(ip_v6_prefix, 'INET')['hex_ipv6_range_start']::VARCHAR AS hex_ipv6_range_start,
    PARSE_IP(ip_v6_prefix, 'INET')['hex_ipv6_range_start']::VARCHAR AS hex_ipv6_range_end,
    COALESCE(hex_ipv4_range_start, hex_ipv6_range_start) AS hex_ip_range_start,
    COALESCE(hex_ipv4_range_end, hex_ipv6_range_end) AS hex_ip_range_end,
    COALESCE(hex_ipv4, hex_ipv6) AS hex_ip,
    CASE
      WHEN ipv4 IS NOT NULL THEN 'ipv4'
      WHEN hex_ipv6 IS NOT NULL THEN 'ipv6'
      ELSE 'unknown'
    END AS ip_type
  FROM source
  INNER JOIN LATERAL FLATTEN(INPUT => PARSE_JSON(source.json_data)) AS meta
  INNER JOIN LATERAL FLATTEN(INPUT => meta.value['prefixes']) AS prefixes
)

SELECT *
FROM renamed
