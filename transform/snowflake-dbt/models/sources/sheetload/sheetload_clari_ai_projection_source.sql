WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'clari_ai_projection') }}

), renamed AS (

    SELECT
      "Date"::VARCHAR                                      AS      projection_date,
      "Projection $"::VARCHAR                              AS      projection_dollar_amount ,
      "EOQ Closed $"::VARCHAR                              AS      eoq_closed_dollar_amount ,
      "Projection Error %"::VARCHAR                        AS      projection_error_percentage ,
      "Projection Lower Bound $"::VARCHAR                  AS      projection_lower_bound_dollar_amount ,
      "Projection Upper Bound $"::VARCHAR                  AS      projection_upper_bound_dollar_amount ,
      "Top 1 Source of Error"::VARCHAR                     AS      top_one_source_of_error ,
      "Top 1 Source of Error $ Diff"::VARCHAR              AS      top_one_source_of_error_dollar_amount_diff ,
      "Top 2 Source of Error"::VARCHAR                     AS      top_two_source_of_error ,
      "Top 2 Source of Error $ Diff"::VARCHAR              AS      top_two_source_of_error_dollar_amount_diff ,
      "Closed as of Date $"::VARCHAR                       AS      closed_as_of_date_dollar_amount ,
      "Closed Converted by EOQ $"::VARCHAR                 AS      closed_converted_by_eoq_dollar_amount ,
      "Projected Closed Converted by EOQ $"::VARCHAR       AS      projected_closed_converted_by_eoq_dollar_amount ,
      "Closed Converted by EOQ %"::VARCHAR                 AS      closed_converted_by_eoq_percentage ,
      "Projected Closed Converted by EOQ %"::VARCHAR       AS      projected_closed_converted_by_eoq_percentage ,
      "New as of Date $"::VARCHAR                          AS      new_as_of_date_dollar_amount ,
      "New Converted by EOQ $"::VARCHAR                    AS      new_converted_by_eoq_dollar_amount ,
      "Projected New Converted by EOQ $"::VARCHAR          AS      projected_new_converted_by_eoq_dollar_amount ,
      "New Converted by EOQ %"::VARCHAR                    AS      new_converted_by_eoq_percentage ,
      "Projected New Converted by EOQ %"::VARCHAR          AS      projected_new_converted_by_eoq_percentage ,
      "Best Case as of Date $"::VARCHAR                    AS      best_case_as_of_date_dollar_amount ,
      "Best Case Converted by EOQ $"::VARCHAR              AS      best_case_converted_by_eoq_dollar_amount ,
      "Projected Best Case Converted by EOQ $"::VARCHAR    AS      projected_best_case_converted_by_eoq_dollar_amount ,
      "Best Case Converted by EOQ %"::VARCHAR              AS      best_case_converted_by_eoq_percentage ,
      "Projected Best Case Converted by EOQ %"::VARCHAR    AS      projected_best_case_converted_by_eoq_percentage ,
      "Commit as of Date $"::VARCHAR                       AS      commit_as_of_date_dollar_amount ,
      "Commit Converted by EOQ $"::VARCHAR                 AS      commit_converted_by_eoq_dollar_amount ,
      "Projected Commit Converted by EOQ $"::VARCHAR       AS      projected_commit_converted_by_eoq_dollar_amount ,
      "Commit Converted by EOQ %"::VARCHAR                 AS      commit_converted_by_eoq_percentage ,
      "Projected Commit Converted by EOQ %"::VARCHAR       AS      projected_commit_converted_by_eoq_percentage ,
      "Omitted as of Date $"::VARCHAR                      AS      omitted_as_of_date_dollar_amount ,
      "Omitted Converted by EOQ $"::VARCHAR                AS      omitted_converted_by_eoq_dollar_amount ,
      "Projected Omitted Converted by EOQ $"::VARCHAR      AS      projected_omitted_converted_by_eoq_dollar_amount ,
      "Omitted Converted by EOQ %"::VARCHAR                AS      omitted_converted_by_eoq_percentage ,
      "Projected Omitted Converted by EOQ %"::VARCHAR      AS      projected_omitted_converted_by_eoq_percentage ,
      "Pipeline as of Date $"::VARCHAR                     AS      pipeline_as_of_date_dollar_amount ,
      "Pipeline Converted by EOQ $"::VARCHAR               AS      pipeline_converted_by_eoq_dollar_amount ,
      "Projected Pipeline Converted by EOQ $"::VARCHAR     AS      projected_pipeline_converted_by_eoq_dollar_amount ,
      "Pipeline Converted by EOQ %"::VARCHAR               AS      pipeline_converted_by_eoq_percentage ,
      "Projected Pipeline Converted by EOQ %"::VARCHAR     AS      projected_pipeline_converted_by_eoq_percentage
    FROM source
)

SELECT *
FROM renamed










































