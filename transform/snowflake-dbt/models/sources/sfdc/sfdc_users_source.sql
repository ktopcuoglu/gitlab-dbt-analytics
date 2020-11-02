WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'user') }}

), renamed AS(

    SELECT

      -- ids
      id               AS user_id,
      name             AS name,
      email            AS user_email,

      -- info
      title            AS title,
      team__c          AS team,
      department       AS department,
      managerid        AS manager_id,
      manager_name__c  AS manager_name,
      isactive         AS is_active,
      userroleid       AS user_role_id,
      start_date__c    AS start_date,
      user_segment__c  AS user_segment,

      --metadata
      createdbyid      AS created_by_id,
      createddate      AS created_date,
      lastmodifiedbyid AS last_modified_id,
      lastmodifieddate AS last_modified_date,
      systemmodstamp,

      --dbt last run
      convert_timezone('America/Los_Angeles',convert_timezone('UTC',current_timestamp())) AS _last_dbt_run
    
    FROM source

)

SELECT *
FROM renamed
