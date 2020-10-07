{{ config({
    "schema": "analytics"
    })
}}

WITH employees as (

    SELECT *
    FROM {{ ref ('bamboohr_directory') }}

), contacts AS (

    SELECT *
    FROM {{ ref ('bamboohr_emergency_contacts') }}

), contacts_aggregated AS (

    SELECT
      employee_id, 
      SUM(IFF(home_phone IS NOT NULL OR mobile_phone IS NOT NULL OR work_phone IS NOT NULL,1,0)) AS total_emergency_contact_numbers
    FROM contacts
    GROUP BY 1

), final AS (

    SELECT 
      employees.*,
      IFF(contacts_aggregated.total_emergency_contact_numbers= 0, TRUE, FALSE) AS missing_contact
    FROM employees
    LEFT JOIN contacts_aggregated
      ON employees.employee_id = contacts_aggregated.employee_id

)

SELECT *
FROM final
WHERE MISSING_CONTACT = TRUE
