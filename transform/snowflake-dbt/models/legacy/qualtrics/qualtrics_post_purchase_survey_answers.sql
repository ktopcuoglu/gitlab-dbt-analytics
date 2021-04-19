WITH responses AS (

    SELECT *
    FROM {{ ref('qualtrics_post_purchase_survey_responses_source') }}

), questions AS (

    SELECT 
      *,
      IFNULL(answer_choices[0]['1']['TextEntry'] = 'on', IFNULL(ARRAY_SIZE(answer_choices) = 0, TRUE)) AS is_free_text
    FROM {{ ref('qualtrics_question') }}

), revised_question_ids AS (
    
    SELECT
      question_description,
      answer_choices,
      IFF(is_free_text, question_id || '_TEXT', question_id) AS question_id
    FROM questions

), parsed_out_qas AS (

    SELECT 
      response_id,
      question_id,
      question_description,
      GET(response_values, question_id)                 AS question_response,
      answer_choices[0]                                 AS answer_choices,
      response_values['distributionChannel']::VARCHAR   AS distribution_channel,
      IFF(response_values['finished'] = 1, True, False) AS has_finished_survey,
      response_values['startDate']::TIMESTAMP           AS survey_start_date,
      response_values['endDate']::TIMESTAMP             AS survey_end_date,
      response_values['recordedDate']::TIMESTAMP        AS response_recorded_at,
      response_values['userLanguage']::VARCHAR          AS user_language,
      GET(response_values, 'plan')::VARCHAR             AS user_plan
    FROM revised_question_ids 
    INNER JOIN responses
      ON GET(response_values, question_id) IS NOT NULL
    WHERE QUESTION_ID != 'QID4_TEXT' AND ARRAY_SIZE(QUESTION_RESPONSE) > 0

), responses_cleaned_up AS (

    SELECT
      GET(answer_choices, d.value)['Display'] AS answer_display,
      d.value::VARCHAR || question_id         AS answer_id,
      response_id,
      question_id
    FROM parsed_out_qas,
    LATERAL FLATTEN(input => question_response) d 
      

), final AS (

    SELECT
      responses_cleaned_up.response_id,
      responses_cleaned_up.question_id,
      question_description,
      answer_display,
      answer_id,
      distribution_channel,
      has_finished_survey,
      survey_start_date,
      survey_end_date,
      response_recorded_at,
      user_language,
      user_plan
    FROM responses_cleaned_up
    INNER JOIN parsed_out_qas 
      ON responses_cleaned_up.response_id = parsed_out_qas.response_id
        AND responses_cleaned_up.question_id = parsed_out_qas.question_id
    WHERE answer_display IS NOT NULL

)

SELECT *
FROM final