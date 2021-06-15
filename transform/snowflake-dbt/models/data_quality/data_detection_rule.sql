{{config({
    "schema": "data_quality"
  })
}}

WITH detection_rule AS (

    SELECT 
          1 as rule_id,
          'Missing instance types' as rule_name,
          'Missing instance types for UUID/hostname' as rule_description,
          'Product' as type_of_data,
          50 AS threshold

   UNION

    SELECT 
          2 as rule_id,
          'Licenses with missing Subscriptions' as rule_name,
          'License IDs that do not have an associated Subscription ID' as rule_description,
          'Product' AS type_of_data,
           50 AS threshold

    UNION

      SELECT 
          3 as rule_id,
          'Subscription with missing Licenses' as rule_name,
          'Subscription IDs that do not have an associated License ID' as rule_description,
          'Product' as type_of_data,
          50 AS threshold

    UNION

      SELECT 
          4 as rule_id,
          'Subscriptions with Self-Managed Plans having License Start dates in the future' as rule_name,
          'Subscription IDs with Self-Managed Plans having license_start_date in the future' as rule_description,
          'Product' as type_of_data,
          50 AS threshold

    UNION

      SELECT 
          5                                                                                                 AS rule_id,
          'Subscriptions with Self-Managed Plans having License Start Date greater than Licese Expire date' AS rule_name,
          'Subscriptions with Self-Managed Plans having license_start_date greater than licese_expire_date' AS rule_description,
          'Product'                                                                                         AS type_of_data,
          50 AS threshold

    UNION

      SELECT 
           6 as rule_id,
          'Expired Licenses' as rule_name,
          'License IDs with Subscription End Dates in the Past' as rule_description,
          'Product' as type_of_data,
          50 AS threshold

    UNION 

      SELECT 
          7 as rule_id,
          'SaaS Subscriptions Not Mapped to Namespaces' as rule_name,
          'SaaS Subscription IDs that do not have any associated Namespace IDs' as rule_description,
          'Product' as type_of_data,
          50 AS threshold
              
)

{{ dbt_audit(
    cte_ref="detection_rule",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2021-06-21",
    updated_date="2021-06-21"
) }}