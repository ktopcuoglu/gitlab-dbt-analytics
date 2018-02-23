with usage_pings as (
  SELECT * FROM {{ ref('usage_data_clean') }}
),

vers_pings as (
  SELECT * FROM {{ ref('version_checks_clean') }}
)

SELECT
  ud.clean_url,
  max(ud.gitlab_version)                                   AS usage_data_gl_version,
  max(ud.host_id)                                          AS usage_data_host_id,
  max(cast(vp.request_data AS TEXT))                       AS ping_data,
  max(vp.ping_count)                                       AS version_ping_count,
  max(cast(ud.stats AS TEXT))                              AS usage_stats,
  max(ud.active_user_count)                                AS active_user_count,
  sum(ud.usage_pings)                                      AS total_usage_pings,
  max(ud.updated_at)                                       AS updated_at,
  'https://version.gitlab.com/servers/' || max(ud.host_id) AS version_link
FROM vers_pings AS vp
  JOIN usage_pings AS ud ON ud.clean_url = vp.clean_url
WHERE vp.updated_at >= (now() - '60 days' :: INTERVAL)
GROUP BY ud.clean_url