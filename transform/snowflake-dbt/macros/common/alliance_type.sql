{%- macro alliance_type(fulfillment_partner_name, fulfillment_partner) -%}

CASE
  WHEN LOWER({{ fulfillment_partner_name }}) LIKE '%google%'
    THEN 'Google Cloud'
  WHEN LOWER({{ fulfillment_partner_name }}) LIKE ANY ('%aws%', '%amazon%')
    THEN 'Amazon Web Services'
  WHEN LOWER({{ fulfillment_partner_name }}) LIKE '%ibm (oem)%'
    THEN 'IBM (OEM)'
  WHEN {{ fulfillment_partner }} IS NOT NULL
    THEN 'Non-Alliance Partners'
END  AS alliance_type

{%- endmacro -%}
