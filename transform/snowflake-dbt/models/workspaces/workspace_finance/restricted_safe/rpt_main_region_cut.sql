{{ config(materialized='table') }}

{{ rpt_main_sales_management_cut_generator(["region_grouped", "sales_segment_grouped"], 'FALSE') }}
