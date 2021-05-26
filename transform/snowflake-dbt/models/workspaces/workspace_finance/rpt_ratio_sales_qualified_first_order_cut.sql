{{ config(materialized='table') }}

{{ rpt_ratio_sales_management_cut_generator(["sales_qualified_source", "sales_segment_grouped"], 'TRUE', "order_type_grouped = '1) New - First Order'") }}
