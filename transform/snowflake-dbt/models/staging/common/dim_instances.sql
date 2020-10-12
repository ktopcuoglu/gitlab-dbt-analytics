WITH usage_ping AS (

    SELECT *
    FROM {{ ref('dim_usage_pings') }}

), instances AS (

    SELECT
        uuid,
        MIN(recorded_at)                    AS recorded_first_usage_ping_time_stamp,  
        MAX(recorded_at)                    AS recorded_most_recent_usage_ping_time_stamp, 
        MIN(instance_user_count)            AS recorded_minimum_instance_user_count,
        MAX(instance_user_count)            AS recorded_maximum_instance_user_count,
        COUNT(DISTINCT id)                  AS recorded_total_usage_pings_sent, 
        COUNT(DISTINCT location_id)         AS recorded_total_countries, 
        COUNT(DISTINCT license_md5)         AS recorded_total_license_count, 
        COUNT(DISTINCT version)             AS recorded_total_version_count, 
        COUNT(DISTINCT edition)             AS recorded_total_edition_count, 
        COUNT(DISTINCT hostname)            AS recorded_total_hostname_count, 
        COUNT(DISTINCT host_id)             AS recorded_total_host_id_count, 
        COUNT(DISTINCT installation_type)   AS recorded_total_installation_type_count
        
    FROM usage_ping
    GROUP BY uuid 

), renamed AS (

    SELECT * 
    FROM instances 

)


{{ dbt_audit(
    cte_ref="renamed",
    created_by="@kathleentam",
    updated_by="@kathleentam",
    created_date="2020-10-11",
    updated_date="2020-10-11"
) }}
