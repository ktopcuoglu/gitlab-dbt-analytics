{%- macro alliance_type_short(partner_account_name, influence_account_name, partner_account, influence_partner) -%}

CASE
  WHEN LOWER({{ partner_account_name }}) LIKE '%google%' OR LOWER({{ influence_account_name }}) LIKE '%google%'
    THEN 'GCP'
  WHEN LOWER({{ partner_account_name }}) LIKE ANY ('%aws%', '%amazon%') OR LOWER({{ influence_account_name }}) LIKE ANY ('%aws%', '%amazon%')
    THEN 'AWS'
  WHEN LOWER({{ partner_account_name }}) LIKE '%ibm (oem)%' OR LOWER({{ influence_account_name }}) LIKE '%ibm (oem)%'
    THEN 'IBM'
  WHEN {{ partner_account }} IS NOT NULL OR {{ influence_partner }} IS NOT NULL
    THEN 'Non-Alliance Partners'
  --ELSE 'Missing alliance_type_short_name'
END  AS alliance_type_short

{%- endmacro -%}
