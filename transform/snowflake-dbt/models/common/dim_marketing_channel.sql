WITH marketing_channel AS(

    SELECT
      dim_marketing_channel_id,
      marketing_channel_name
    FROM {{ ref('prep_marketing_channel') }}
)

{{ dbt_audit(
    cte_ref="marketing_channel",
    created_by="@paul_armstrong",
    updated_by="@mcooperDD",
    created_date="2020-11-13",
    updated_date="2020-12-18"
) }}
