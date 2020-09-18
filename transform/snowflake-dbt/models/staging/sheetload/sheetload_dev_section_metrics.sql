WITh dev_section_metrics AS (

    SELECT *
    FROM {{ ref('sheetload_dev_section_metrics_source') }}

)

SELECT *
FROM dev_section_metrics
