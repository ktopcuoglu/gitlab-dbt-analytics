{{ config({
        "materialized": "incremental",
        "unique_key": "crm_opportunity_snapshot_id",
        "tags": ["opportunity_snapshots"],
    })
}}

{{ sfdc_opportunity_fields('snapshot') }}

), additional_calcs AS (

  SELECT 
    CASE
      WHEN final.pipeline_created_fiscal_quarter_name = final.snapshot_fiscal_quarter_name
        AND final.is_eligible_created_pipeline = 1
        THEN final.net_arr
      ELSE 0
    END AS created_in_snapshot_quarter_net_arr,
    CASE
      WHEN final.pipeline_created_fiscal_quarter_name = final.close_fiscal_quarter_name
        AND final.is_won = 1
        AND final.is_eligible_created_pipeline = 1
        THEN final.net_arr
      ELSE 0
    END AS created_and_won_same_quarter_net_arr,
    CASE
      WHEN final.pipeline_created_fiscal_quarter_name = final.snapshot_fiscal_quarter_name
        AND final.is_eligible_created_pipeline = 1
        THEN final.calculated_deal_count
      ELSE 0
    END AS created_in_snapshot_quarter_deal_count,
    CASE
      WHEN is_open = 1
          THEN DATEDIFF(days, final.created_date, final.snapshot_date)
        WHEN is_open = 0 AND final.snapshot_date < final.close_date
          THEN DATEDIFF(days, final.created_date, final.snapshot_date)
        ELSE DATEDIFF(days, final.created_date, final.close_date)
      END                                                       AS calculated_age_in_days
  FROM final
)

{{ dbt_audit(
    cte_ref="additional_calcs",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-02-23",
    updated_date="2022-07-28"
) }}
