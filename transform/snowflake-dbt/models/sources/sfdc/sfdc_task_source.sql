WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'task') }}

), renamed AS(

    SELECT
      id                            AS task_id,

      --keys
      accountid                     AS account_id,
      ownerid                       AS owner_id,
      whoid                         AS lead_or_contact_id,
      whatid                        AS account_or_opportunity_id,

      --info
      description                   AS full_comments,
      subject                       AS task_subject,
      activitydate                  AS task_date,
      isdeleted                     AS is_deleted,
      status                        AS status,              
      type                          AS type,
      createddate                   AS task_created_date,
      createdbyid                   AS task_created_by_id,

      assigned_employee_number__c   AS assigned_employee_number,
      -- Original issue: https://gitlab.com/gitlab-data/analytics/-/issues/6577
      persona_functions__c          AS persona_functions,
      persona_levels__c             AS persona_levels,
      sa_activity_type__c           AS sa_activity_type,
      gs_activity_type__c           AS gs_activity_type

    FROM source
)

SELECT *
FROM renamed
