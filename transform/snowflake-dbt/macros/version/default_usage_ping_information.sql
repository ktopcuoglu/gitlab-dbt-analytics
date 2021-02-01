{% macro default_usage_ping_information() %} 

    -- usage ping meta data 
    dim_usage_ping_id, 
    ping_created_at,
    ping_created_at_28_days_earlier,
    ping_created_at_year,
    ping_created_at_month,
    ping_created_at_week,
    ping_created_at_date,

    -- instance settings 
    raw_usage_data_payload['uuid']::VARCHAR                                                                 AS uuid, 
    ping_source, 
    raw_usage_data_payload['version']::VARCHAR                                                              AS instance_version, 
    cleaned_version,
    version_is_prerelease,
    major_version,
    minor_version,
    major_minor_version,
    edition, 
    main_edition, 
    raw_usage_data_payload['hostname']::VARCHAR                                                             AS hostname, 
    raw_usage_data_payload['host_id']::NUMBER(38,0)                                                         AS host_id, 
    raw_usage_data_payload['installation_type']::VARCHAR                                                    AS installation_type, 
    is_internal, 
    is_staging,    

    -- instance user statistics 
    raw_usage_data_payload['instance_user_count']::NUMBER(38,0)                                             AS instance_user_count, 
    raw_usage_data_payload['historical_max_users']::NUMBER(38,0)                                            AS historical_max_users, 
    raw_usage_data_payload['license_md5']::VARCHAR                                                          AS license_md5,

    
{%- endmacro -%}

