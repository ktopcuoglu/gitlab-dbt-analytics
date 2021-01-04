WITH gitlab_ops_users_xf AS (

    SELECT * 
    FROM {{ref('gitlab_ops_users_xf')}} 

), final AS (

    SELECT 
      user_id, 
      user_name                                                AS gitlab_ops_user_name,
      notification_email, 
      SPLIT_PART(notification_email,'@', 0)                    AS email_handle, 
      COUNT(notification_email) OVER (PARTITION BY user_id)    AS number_of_emails 
    FROM gitlab_ops_users_xf
    WHERE length (email_handle) > 1       -- removes records with just one number  
      AND notification_email ILIKE '%gitlab.com'
      AND email_handle NOT LIKE '%~%'     -- removes admin accounts 
      AND email_handle NOT LIKE '%+%'     -- removes any emails with special character + 
      AND email_handle NOT LIKE '%admin%' -- removes records with the word admin
      AND email_handle NOT LIKE '%hack%'  -- removes hack accounts
      AND email_handle NOT LIKE '%xxx%'   -- removes accounts with more than three xs
      AND email_handle NOT LIKE '%gitlab%'-- removes accounts that have the word gitlab
      AND email_handle NOT LIKE '%test%'  -- removes accounts with more than three xs
    GROUP BY 1,2,3,4

)

SELECT * 
FROM final 
