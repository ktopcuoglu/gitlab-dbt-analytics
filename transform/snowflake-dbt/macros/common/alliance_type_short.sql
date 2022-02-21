{%- macro alliance_type_short(fulfillment_partner_name, fulfillment_partner) -%}

CASE
  WHEN LOWER({{ fulfillment_partner_name }}) LIKE '%google%'
    THEN 'GCP'
  WHEN LOWER({{ fulfillment_partner_name }}) LIKE ANY ('%aws%', '%amazon%')
    THEN 'AWS'
  WHEN LOWER({{ fulfillment_partner_name }}) LIKE '%ibm (oem)%'
    THEN 'IBM'
  WHEN {{ fulfillment_partner }} IS NOT NULL
    THEN 'Non-Alliance'
END  AS alliance_type_short

{%- endmacro -%}
