{%- macro channel_type(dr_partner_engagement, order_type) -%}

CASE
  WHEN {{ dr_partner_engagement }} = 'Partner Sourced'
    AND {{ order_type }} = '1. New - First Order'
    THEN 'Chan Sourced - New'
  WHEN {{ dr_partner_engagement }} = 'Partner Sourced'
    AND {{ order_type }} != '1. New - First Order'
    THEN 'Chan Sourced - Growth'
  WHEN {{ dr_partner_engagement }} = 'Assisted'
    THEN 'Assist'
  WHEN {{ dr_partner_engagement }} = 'Fulfillment'
    THEN 'Fulfilled'
  ELSE 'Missing channel_type'
END AS channel_type

{%- endmacro -%}
