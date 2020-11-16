{{ config({
        "materialized": "view",
        "schema": "common_mapping"
    })
}}
WITH touchpoints AS (

  SELECT *
  FROM {{ ref('sfdc_bizible_touchpoint_source') }}

), final AS (

    SELECT DISTINCT
      bizible_marketing_channel_path                                    AS bizible_marketing_channel_path,
      {{ map_marketing_channel_path("bizible_marketing_channel_path")}} AS marketing_channel_name
    FROM touchpoints
    WHERE bizible_touchpoint_position LIKE '%FT%'

)


{{ dbt_audit(
    cte_ref="final",
    created_by="@paul_armstrong",
    updated_by="@paul_armstrong",
    created_date="2020-11-13",
    updated_date="2020-11-13"
) }}
