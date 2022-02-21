{{ config(materialized='table') }}

{{ rpt_main_sales_management_cut_generator(["segment_region_grouped"], 'FALSE') }}
