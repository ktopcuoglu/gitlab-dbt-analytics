{{config({ 
    "schema": "data_quality" 
 })
}}


WITH detection_rule AS (

 SELECT * 
 FROM {{ ref('product_data_detection_run_result') }}

), rule_run_detail AS ( 
    
    SELECT * 
    FROM {{ ref('product_data_detection_run_detail') }}

)



SELECT 
    detection_rule.rule_id, 
    detection_rule.rule_name, 
    detection_rule.rule_description, 
    rule_run_detail.rule_run_date, 
    rule_run_detail.processed_record_count,
    rule_run_detail.passed_record_count, 
    rule_run_detail.failed_record_count,

 rule_run_detail.type_of_dataFROM rule_run_detailLEFT OUTER JOIN detection_rule ONrule_id = rule_id