{{ config({
    "materialized": "view"
    })
}}

{{ dbt_utils.union_relations(
    relations=[ref('sheetload_cert_product_geo_dashboard_source'),
               ref('sheetload_cert_product_geo_sql_source'),
               ref('sheetload_cert_customer_segmentation_dashboard_source'),
               ref('sheetload_cert_customer_segmentation_sql_source'),
               ref('sheetload_communication_certificate'),
               ref('sheetload_ic_collaboration_competency'),
               ref('sheetload_ic_dib_comptency'),
               ref('sheetload_ic_efficiency_competency'),
               ref('sheetload_ic_iteration_competency'),
               ref('sheetload_ic_results_competency'),
               ref('sheetload_ic_transparency_competency'),
               ref('sheetload_compensation_certificate')]
) }}
