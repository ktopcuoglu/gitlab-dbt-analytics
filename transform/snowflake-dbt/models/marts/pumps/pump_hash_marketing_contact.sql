select 
  SHA2(email_address) AS hashed_email,
  {{ dbt_utils.star(from=ref('mart_marketing_contact'), except=["email_address"]) }}
from {{ref('mart_marketing_contact')}}