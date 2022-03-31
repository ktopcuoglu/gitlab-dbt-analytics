{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('dim_namespace', 'dim_namespace'),
    ('dim_date','dim_date'),
    ('fct_usage_event', 'fct_usage_event'),
    ('xmau_metrics', 'gitlab_dotcom_xmau_metrics'),
    ])
}}

, fact_raw AS (

    SELECT
        event_id,
        CAST(EVENT_CREATED_AT AS DATE)                                                                  AS event_date,
        dim_user_id,
        event_name,
        dim_product_tier_id,
        dim_subscription_id,
        dim_crm_account_id,
        dim_billing_account_id,
        stage_name,
        section_name,
        group_name,
        data_source,
        plan_id_at_event_date,
        plan_name_at_event_date,
        plan_was_paid_at_event_date,
        dim_namespace_id,
        DATE_TRUNC('MONTH', event_date)                                                                 AS reporting_month,
        QUARTER(event_date)                                                                             AS reporting_quarter,
        YEAR(event_date)                                                                                AS reporting_year

    FROM fct_usage_event as fact

), fact_with_date_range AS (

    SELECT
        fact.event_id,
        fact.event_date,
        dim_date.last_day_of_month                                                                      AS last_day_of_month,
        dim_date.last_day_of_quarter                                                                    AS last_day_of_quarter,
        dim_date.last_day_of_fiscal_year                                                                AS last_day_of_fiscal_year,
        fact.dim_user_id,
        fact.event_name,
        fact.dim_product_tier_id,
        fact.dim_subscription_id,
        fact.dim_crm_account_id,
        fact.dim_billing_account_id,
        fact.stage_name,
        fact.section_name,
        fact.group_name,
        fact.data_source,
        fact.plan_id_at_event_date,
        fact.plan_name_at_event_date,
        fact.plan_was_paid_at_event_date,
        fact.dim_namespace_id,
        fact.reporting_month,
        fact.reporting_quarter,
        fact.reporting_year
    FROM fact_raw as fact
    LEFT JOIN dim_date
        ON fact.event_date = dim_date.DATE_ACTUAL
    WHERE fact.event_date BETWEEN DATEADD('day', -27, last_day_of_month) AND last_day_of_month

), fact_with_namespace AS (

    SELECT
        fact.*,
        CAST(namespace.created_at AS DATE)                                                              AS namespace_created_at,
        DATEDIFF(day, namespace_created_at,GETDATE())                                                   AS days_since_namespace_created
    FROM fact_with_date_range AS fact
    LEFT JOIN dim_namespace AS namespace
        ON fact.dim_namespace_id = namespace.dim_namespace_id

), fact_with_xmau_flags AS (

    SELECT
        fact.*,
        xmau.smau                                                                                       AS is_smau,
        xmau.gmau                                                                                       AS is_gmau,
        xmau.is_umau                                                                                    AS is_umau
    FROM fact_with_namespace AS fact
    LEFT JOIN xmau_metrics AS xmau
        ON fact.event_name = xmau.events_to_include

), results AS (

    SELECT
        {{ dbt_utils.surrogate_key(['reporting_month', 'plan_id_at_event_date', 'event_name']) }}       AS mart_usage_event_plan_monthly_id,
        reporting_month,
        plan_id_at_event_date,
        event_name,
        stage_name,
        section_name,
        group_name,
        is_smau,
        is_gmau,
        is_umau,
        --reporting_quarter, (commented out to reduce table size. If want to look at
        --reporting_year,     quarter or yearly usage, uncomment and add to surrogate key)
        COUNT(*)                                                                                        AS event_count,
        COUNT(DISTINCT(dim_namespace_id))                                                               AS namespace_count,
        COUNT(DISTINCT(dim_user_id))                                                                        AS user_count
    FROM fact_with_xmau_flags
    {{ dbt_utils.group_by(n=10) }}
    ORDER BY reporting_month DESC, plan_id_at_event_date DESC

)

{{ dbt_audit(
    cte_ref="results",
    created_by="@dihle",
    updated_by="@dihle",
    created_date="2022-02-22",
    updated_date="2022-02-23"
) }}
