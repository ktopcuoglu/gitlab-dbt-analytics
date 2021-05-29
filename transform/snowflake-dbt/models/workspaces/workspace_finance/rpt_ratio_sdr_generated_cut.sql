{{ config(materialized='table') }}

{{ rpt_ratio_sales_management_cut_generator(["segment_region_grouped", "order_type_grouped"], 'FALSE', "sales_qualified_source='SDR Generated'") }}
