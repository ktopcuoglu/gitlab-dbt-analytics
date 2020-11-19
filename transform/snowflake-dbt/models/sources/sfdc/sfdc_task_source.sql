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

      --info
      subject                       AS task_subject,
      activitydate                  AS task_date,
      isdeleted                     AS is_deleted,

      assigned_employee_number__c   AS assigned_employee_number,

      persona_functions__c          AS persona_functions,
      persona_levels__c             AS persona_levels,
      sa_activity_type__c           AS sa_activity_type

    FROM source
)

SELECT *
FROM renamed


-- opportunity?
-- full_comments?
