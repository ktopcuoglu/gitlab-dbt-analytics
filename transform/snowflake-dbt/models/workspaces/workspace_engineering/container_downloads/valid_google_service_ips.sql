{{
  simple_cte([
    ('ip_ranges','sheetload_google_cloud_ip_ranges_source'),
    ('user_ips','sheetload_google_user_ip_ranges_source')
  ])
}},
/*
https://support.google.com/a/answer/10026322?hl=en
Obtain Google IP address ranges

As an administrator, you can use these lists when you need a range of IP addresses for Google APIs and services' default domains: 

- IP ranges that Google makes available to users on the internet
- Global and regional external IP address ranges for customers' Google Cloud resources

The default domains' IP address ranges for Google APIs and services fit within the list of ranges between these 2 sources. (Subtract the usable ranges from the complete list.) 
*/
gcp_ranges AS (

  SELECT
    ip_ranges.*,
    IFF(user_ips.hex_ip is null,TRUE,FALSE) AS is_user_available 
  FROM
  ip_ranges
  LEFT JOIN user_ips
    ON ip_ranges.hex_ip = user_ips.hex_ip

)

SELECT *
FROM gcp_ranges
