{{ config({
    "materialized": "table"
    })
}}

{{simple_cte([
                ('wk_saas_spo', 'wk_saas_spo'), 
                ('wk_self_managed_spo', 'wk_self_managed_spo')
                ]
                )}}

SELECT *
FROM wk_self_managed_spo

UNION

SELECT *
FROM wk_saas_spo
