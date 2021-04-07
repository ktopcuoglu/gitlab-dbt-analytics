{%- macro alliance_type(partner_account_name, influence_account_name, partner_account, influence_partner) -%}

CASE
  WHEN LOWER({{ partner_account_name }}) LIKE '%google%' OR LOWER({{ influence_account_name }}) LIKE '%google%'
    THEN 'Google Cloud'
  WHEN LOWER({{ partner_account_name }}) LIKE ANY ('%aws%', '%amazon%') OR LOWER({{ influence_account_name }}) LIKE ANY ('%aws%', '%amazon%')
    THEN 'Amazon Web Services'
  WHEN LOWER({{ partner_account_name }}) LIKE '%ibm (oem)%' OR LOWER({{ influence_account_name }}) LIKE '%ibm (oem)%'
    THEN 'IBM (OEM)'
  WHEN {{ partner_account }} IS NOT NULL OR {{ influence_partner }} IS NOT NULL
    THEN 'Non-Alliance Partners'
  ELSE 'Missing alliance_type_name'
END                              AS alliance_type

{%- endmacro -%}
