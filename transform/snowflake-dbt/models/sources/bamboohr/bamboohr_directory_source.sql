WITH source AS (

    SELECT *
	  FROM {{ source('bamboohr', 'directory') }}
	
), intermediate AS (

    SELECT
      data_by_row['id']::NUMBER 				        AS employee_id,
	    data_by_row['displayName']::VARCHAR 	    AS full_name,
      data_by_row['jobTitle']::VARCHAR 			    AS job_title,
	    data_by_row['supervisor']::VARCHAR 		    AS supervisor,
	    data_by_row['workEmail']::VARCHAR			    AS work_email,
      uploaded_at                               AS uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => TRUE) initial_unnest,
    LATERAL FLATTEN(INPUT => parse_json(initial_unnest.value), OUTER => true) data_by_row


), final AS (

    SELECT *
    FROM intermediate
    WHERE work_email != 't2test@gitlab.com'
    
)

SELECT *
FROM final
