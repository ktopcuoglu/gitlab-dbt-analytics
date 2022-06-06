{{ config(
    tags=["mnpi_exception", "pmg"]
) }}

    SELECT
      1
    FROM {{ ref('mart_product_usage_free_user_metrics_monthly')}}
