{{ simple_cte([('version_hosts_source', 'version_hosts_source')]) }}

, renamed AS (

  SELECT 
    host_id  AS dim_host_id,
    host_url AS host_name 
  FROM version_hosts_source 

)

SELECT *
FROM renamed
