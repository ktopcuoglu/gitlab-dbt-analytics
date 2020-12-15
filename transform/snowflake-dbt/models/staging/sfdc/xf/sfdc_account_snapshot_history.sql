WITH sfdc_account_snapshots AS (

    SELECT *
    FROM {{ref('sfdc_account_snapshots_base_clean')}}

)

SELECT *
FROM sfdc_account_snapshots


