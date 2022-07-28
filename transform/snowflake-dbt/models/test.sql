{{
  config(
    materialized='table',
    snowflake_warehouse= generate_warehouse_name('XL') 
  )
}}


SELECT 1

{{ generate_warehouse_name('XL') }}