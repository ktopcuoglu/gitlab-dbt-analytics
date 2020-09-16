WITH source AS (

  SELECT *
  FROM {{ source('sheetload', 'days_sales_outstanding') }}

), renamed AS (

  SELECT
    period::DATE                            AS period,
    total_touch_billings::NUMBER            AS total_touch_billings,
    total_beginning_ar::NUMBER              AS total_beginning_ar,
    total_ar_at_end_of_period::NUMBER       AS total_ar_at_end_of_period,
    nbr_of_days_in_period::NUMBER           AS nbr_of_days_in_period,
    dso::NUMBER                             AS dso,
    collections_based_on_formula::NUMBER    AS collections_based_on_formula,
    total_current_ar_close::NUMBER          AS total_current_ar_close,
    p_d_ar::NUMBER                          AS p_d_ar,
    collection_effectiveness_index::NUMBER  AS collection_effectiveness_index,
    dso_trend::NUMBER                       AS dso_trend,
    cei_trend::NUMBER                       AS cei_trend
  FROM source

)

SELECT *
FROM renamed
