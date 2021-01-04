{{ config(materialized='view') }}

WITH gitlab_dotcom_gitlab_emails AS (
  
    SELECT *
    FROM {{ ref('gitlab_dotcom_gitlab_emails') }}
     
), cleaned AS (

    SELECT 
      user_id, 
      email_address, 
      SPLIT_PART(email_address,'@', 0)                    AS email_handle, 
      COUNT(email_address) OVER (PARTITION BY user_id)    AS number_of_emails 
    FROM gitlab_dotcom_gitlab_emails
    WHERE length (email_handle) > 1       -- removes records with just one number  
      AND email_handle IS NOT NULL 
      AND email_handle NOT LIKE '%~%'     -- removes emails with special character ~
      AND email_handle NOT LIKE '%+%'     -- removes any emails with special character + 
      AND email_handle NOT LIKE '%admin%' -- removes records with the word admin
      AND email_handle NOT LIKE '%hack%'  -- removes hack accounts
      AND email_handle NOT LIKE '%xxx%'   -- removes accounts with more than three xs
      AND email_handle NOT LIKE '%gitlab%'-- removes accounts that have the word gitlab
      AND email_handle NOT LIKE '%test%'  -- removes accounts with test in the name
      AND email_handle NOT IN (           -- removes duplicate emails 
                                'mckai.javeion',
                                'deandre',
                                'gopals',
                                'kenny',
                                'jason'
                              )
    GROUP BY 1,2,3

)

SELECT *
FROM cleaned

