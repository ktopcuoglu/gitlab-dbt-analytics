{{ config(
    tags=["mnpi_exception"]
) }}

-- NEEDS TO BE MOVED TO RESTRICTED SCHEMA FOR MNPI

WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_manual_arr_true_up_allocation_source') }}

)

SELECT *
FROM source