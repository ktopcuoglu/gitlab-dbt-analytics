WITH qualtrics_distribution AS (

    SELECT *
    FROM {{ ref('qualtrics_distribution') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY distribution_id ORDER BY uploaded_at DESC) = 1

), email_sent_count AS (

    SELECT 
      SUM(email_sent_count) AS number_of_emails_sent,
      survey_id
    FROM qualtrics_distribution
    GROUP BY survey_id

)
SELECT *
FROM email_sent_count
