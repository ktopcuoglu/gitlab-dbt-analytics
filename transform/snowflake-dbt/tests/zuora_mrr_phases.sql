-- This test compares MRR at different points of the analysis and makes sure they are all equal. 
-- Because of how MRR is calculated it is necessary that this test be *perfect* for the past months. 
-- While this doesn't track changes over time, there are fluctuations in the future introduces by 
-- the changing ways of how we're calculating MRR.

with zuora_base_mrr_amortized as (
     SELECT * FROM {{ref('zuora_base_mrr_amortized')}}
), zuora_base_trueups as (
    SELECT * FROM {{ref('zuora_base_trueups')}}
), zuora_mrr_totals as (
    SELECT * FROM {{ref('zuora_mrr_totals')}}
), mrr_totals_levelled as (
    SELECT * FROM {{ref('mrr_totals_levelled')}}
), unioned as (
     SELECT mrr_month, mrr::float as mrr FROM zuora_base_mrr_amortized
     UNION ALL
     SELECT trueup_month, mrr::float as mrr FROM zuora_base_trueups
), sum_zuora_base as (
    SELECT mrr_month, sum(mrr) as sum_zuora_base
    FROM unioned
    GROUP BY 1
), sum_mrr_totals as (
    SELECT mrr_month, sum(mrr) as sum_mrr_totals
    FROM zuora_mrr_totals
    GROUP BY 1
), sum_mrr_totals_levelled as (
    SELECT mrr_month, sum(mrr) as sum_mrr_totals_levelled
    FROM mrr_totals_levelled
    GROUP BY 1
)
SELECT sum_zuora_base.mrr_month,
       sum_zuora_base,
       sum_mrr_totals,
       sum_mrr_totals_levelled
FROM sum_zuora_base
FULL OUTER JOIN sum_mrr_totals
    ON sum_zuora_base.mrr_month = sum_mrr_totals.mrr_month
FULL OUTER JOIN sum_mrr_totals_levelled
    ON sum_zuora_base.mrr_month = sum_mrr_totals_levelled.mrr_month
WHERE (sum_mrr_totals - sum_mrr_totals_levelled) > 1
AND sum_zuora_base.mrr_month < date_trunc('month',current_date)