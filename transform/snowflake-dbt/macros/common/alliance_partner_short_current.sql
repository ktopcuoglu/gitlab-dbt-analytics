{%- macro alliance_partner_short_current(fulfillment_partner_name, partner_account_name, resale_partner_track, partner_track, deal_path) -%}

CASE
  WHEN LOWER({{ fulfillment_partner_name }}) LIKE '%google%' OR LOWER({{ partner_account_name }}) LIKE '%google%'
    THEN 'GCP' 
  WHEN LOWER({{ fulfillment_partner_name }}) LIKE ANY ('%aws%', '%amazon%') OR LOWER({{ partner_account_name }}) LIKE ANY ('%aws%', '%amazon%')
    THEN 'AWS'
  WHEN LOWER({{ fulfillment_partner_name }}) LIKE '%ibm (oem)%' OR LOWER({{ partner_account_name }}) LIKE '%ibm (oem)%'
    THEN 'IBM'
  WHEN {{ resale_partner_track }} != 'Technology' AND {{ partner_track }} != 'Technology' AND {{ deal_path }} = 'Channel'
    THEN 'Channel Partners'
END

{%- endmacro -%}
