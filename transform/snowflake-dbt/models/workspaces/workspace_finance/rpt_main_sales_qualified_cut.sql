{{ config(materialized='table') }}

{{ rpt_main_sales_management_cut_generator(["sales_segment_grouped", "sales_qualified_source"], 'TRUE') }}
