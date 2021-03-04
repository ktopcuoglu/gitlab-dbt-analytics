select 
  SHA2(email_address) || '@example.com' AS hashed_email,
  {{ dbt_utils.star(from=ref('mart_marketing_contact'), except=["EMAIL_ADDRESS"]) }}
from {{ref('mart_marketing_contact')}}