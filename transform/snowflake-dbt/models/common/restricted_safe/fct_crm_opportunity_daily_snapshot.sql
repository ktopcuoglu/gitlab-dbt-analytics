{{ config({
        "materialized": "incremental",
        "unique_key": "crm_opportunity_snapshot_id",
        "tags": ["opportunity_snapshots"],
    })
}}

{{ sfdc_opportunity_fields('snapshot') }}

, additional_calcs AS (

  SELECT 
    final.*,
    CASE
      WHEN final.pipeline_created_fiscal_quarter_name = final.snapshot_fiscal_quarter_name
        AND final.is_eligible_created_pipeline = 1
        THEN final.net_arr
      ELSE 0
    END AS created_in_snapshot_quarter_net_arr,
    CASE
      WHEN final.pipeline_created_fiscal_quarter_name = final.snapshot_fiscal_quarter_name
        AND final.is_eligible_created_pipeline = 1
        THEN final.calculated_deal_count
      ELSE 0
    END AS created_in_snapshot_quarter_deal_count
  FROM final
)

{{ dbt_audit(
    cte_ref="additional_calcs",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-02-23",
    updated_date="2022-07-28"
) }}
