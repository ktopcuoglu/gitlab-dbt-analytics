SELECT 
  SHA2(email_address) || '@example.com' AS email_address,
  {{ dbt_utils.star(from=ref('pump_marketing_contact'), except=["EMAIL_ADDRESS"]) }}
FROM {{ref('pump_marketing_contact')}}