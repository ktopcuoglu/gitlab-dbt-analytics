{{ config({
        "materialized": "view",
    })
}}

WITH final AS (

   SELECT 6543 as ultimate_parent_namespace_id
   UNION ALL
   SELECT 9970
   UNION ALL
   SELECT 4347861
   UNION ALL
   SELECT 1400979
   UNION ALL
   SELECT 2299361
   UNION ALL
   SELECT 1353442
   UNION ALL
   SELECT 349181
   UNION ALL
   SELECT 3455548
   UNION ALL
   SELECT 3068744
   UNION ALL
   SELECT 5362395
   UNION ALL
   SELECT 4436569
   UNION ALL
   SELECT 3630110
   UNION ALL
   SELECT 3315282
   UNION ALL
   SELECT 5811832
   UNION ALL
   SELECT 5496509
   UNION ALL
   SELECT 4206656
   UNION ALL
   SELECT 5495265
   UNION ALL
   SELECT 5496484
   UNION ALL
   SELECT 2524164
   UNION ALL
   SELECT 4909902

)


{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2020-12-29",
    updated_date="2020-12-29"
) }}
