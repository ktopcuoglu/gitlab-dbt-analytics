{{ config({
    "alias": "snowplow_gitlab_good_events_source",
    "post-hook": '{{ apply_dynamic_data_masking(columns = [{"app_id":"string"},{"event_id":"string"},{"txn_id":"string"},{"user_id":"string"},{"user_ipaddress":"string"},{"domain_userid":"string"},{"domain_sessionidx":"string"},{"network_userid":"string"},{"geo_latitude":"string"},{"geo_longitude":"string"},{"page_url":"string"},{"page_title":"string"},{"page_referrer":"string"},{"page_urlhost":"string"},{"page_urlport":"string"},{"page_urlpath":"string"},{"page_urlquery":"string"},{"contexts":"string"},{"unstruct_event":"string"},{"tr_orderid":"string"},{"ti_orderid":"string"},{"useragent":"string"},{"mkt_clickid":"string"},{"refr_domain_userid":"string"},{"domain_sessionid":"string"}]) }}'
}) }}

WITH source as (

    SELECT *
    FROM {{ source('gitlab_snowplow', 'events') }}

)

SELECT *
FROM source
