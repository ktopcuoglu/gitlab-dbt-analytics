WITH sfdc_opportunity_xf AS (

    SELECT * FROM {{ref('sfdc_opportunity_xf')}}

) 
SELECT *
FROM sfdc_opportunity_xf