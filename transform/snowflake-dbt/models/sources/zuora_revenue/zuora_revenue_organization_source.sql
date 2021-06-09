WITH zuora_revenue_organization AS (

    SELECT *
    FROM {{source('zuora_revenue','zuora_revenue_organization')}}

), renamed AS (

    SELECT 
    
      id::VARCHAR               AS zuora_revenue_organizaiton_id,
      org_id::VARCHAR           AS organization__id,
      org_name::VARCHAR         AS organizaiton_name,
      crtd_by::VARCHAR          AS organizaiton_created_by,
      crtd_dt::DATE             AS organization_created_date,
      updt_by::VARCHAR          AS organization_updated_by,
      updt_dt::DATE             AS organization_updated_date,
      client_id::VARCHAR        AS client_id,
      crtd_prd_id::VARCHAR      AS organization_created_period_id,
      incr_updt_dt::DATE        AS incremental_update_date,
      org_code::VARCHAR         AS organization_code,
      entity_id::VARCHAR        AS entity_id

    FROM zuora_revenue_organization

)

SELECT *
FROM renamed