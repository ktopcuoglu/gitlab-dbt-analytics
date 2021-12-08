{%- macro pte_base_query(model_run_type) -%}

{% set period_type = 'MONTH'%}
{% set delta_value = 3 %}
-- Prediction date offset by -1 to ensure its only predicting with complete days.
{% set prediction_date = (modules.datetime.datetime.now() - modules.datetime.timedelta(days=1)).date()  %}


{%- if model_run_type=='training' -%}
{% set end_date = modules.datetime.datetime(prediction_date.year, prediction_date.month - delta_value, prediction_date.day).date() %}
{% endif %}
{% if model_run_type=='scoring'  %}
{% set end_date = prediction_date %}
{% endif %}

--Snapshot for just the "current" ARR month based on SNAPSHOT_DT
WITH mart_arr_snapshot_bottom_up AS (

    SELECT *
    FROM {{ ref('mart_arr_snapshot_bottom_up') }}

    ), period_1 AS (

    SELECT dim_crm_account_id
        , COUNT(dim_subscription_id) AS num_of_subs
        , MAX(crm_account_tsp_region) AS crm_account_tsp_region
        , MAX(parent_crm_account_sales_segment) AS parent_crm_account_sales_segment
        , MAX(parent_crm_account_industry) AS parent_crm_account_industry
        , MAX(parent_crm_account_billing_country) AS parent_crm_account_billing_country
        , MAX(parent_crm_account_owner_team) AS parent_crm_account_owner_team
        , MAX(CASE WHEN parent_crm_account_sales_territory !='Territory Not Found' THEN parent_crm_account_sales_territory END) AS parent_crm_account_sales_territory
        , MAX(parent_crm_account_tsp_region) AS parent_crm_account_tsp_region
        , MAX(parent_crm_account_tsp_sub_region) AS parent_crm_account_tsp_sub_region
        , MAX(parent_crm_account_tsp_area) AS parent_crm_account_tsp_area
        , MAX(parent_crm_account_tsp_account_employees) AS crm_account_tsp_account_employees
        , MAX(parent_crm_account_tsp_max_family_employees) AS parent_crm_account_tsp_max_family_employees
        , MAX(parent_crm_account_employee_count_band) AS parent_crm_account_employee_count_band
        , MAX(CASE WHEN product_tier_name LIKE '%Ultimate%' THEN 1 ELSE 0 END) AS is_ultimate_product_tier
        , MAX(CASE WHEN product_tier_name LIKE '%Premium%' THEN 1 ELSE 0 END) AS is_premium_product_tier
        , MAX(CASE WHEN product_tier_name LIKE '%Starter%' or product_tier_name LIKE '%Bronze%' THEN 1 ELSE 0 END) AS is_starter_bronze_product_tier
        , MAX(CASE WHEN service_type = 'Full Service' THEN 1 ELSE 0 END) AS is_service_type_full_service
        , MAX(CASE WHEN service_type = 'Support Only' THEN 1 ELSE 0 END) AS is_service_type_support_only
        , MIN(CASE WHEN term_start_date <= '{{ end_date }}' then DATEDIFF(month, term_start_date, '{{ end_date }}') ELSE -1 END) AS subscription_months_into
        , MIN(DATEDIFF(month, '{{ end_date }}', term_end_date)) AS subscription_months_remaining
        , MIN(DATEDIFF(month, subscription_start_date, subscription_end_date)) AS subscription_duration_in_months
        , MIN(DATEDIFF(month, parent_account_cohort_month, '{{ end_date }}')) AS account_tenure_in_months
        , AVG(health_number) AS health_number
        , SUM(mrr) AS sum_mrr
        , SUM(arr) AS sum_arr
        , SUM(quantity) AS license_count
        , SUM(CASE WHEN product_delivery_type = 'Self-Managed' THEN 1 ELSE 0 END) AS self_managed_instance_count
        , SUM(CASE WHEN product_delivery_type = 'SaaS' THEN 1 ELSE 0 END) AS saas_instance_count
        , SUM(CASE WHEN product_delivery_type = 'Others' THEN 1 ELSE 0 END) AS others_instance_count
        , COUNT(DISTINCT(product_tier_name)) AS num_products_purchased
        , SUM(CASE WHEN subscription_status = 'Cancelled' OR (subscription_status = 'Active' AND subscription_end_date <= dateadd(MONTH,-3,'{{ end_date }}')) THEN 1 ELSE 0 END) AS cancelled_subs --added 3 months before counting active subscriptions as cancelled per Israel's feedback
    FROM mart_arr_snapshot_bottom_up
    -- Contains true-up snapshots for every date from 2020-03-01 to Present. MART_ARR_SNAPSHOT_MODEL contained non-true-up data but contains misisng data prior to 2021-06
    WHERE snapshot_date = '{{ end_date }}' -- limit to snapshot to day before our prediction window
        AND ARR_MONTH = date_trunc('MONTH', cast('{{ end_date }}' as date)) -- limit data for just the month the '{{ end_date }}' falls in. arr_month is unique at the dim_crm_account_id & snapshot_date level
        AND is_jihu_account != 'TRUE' -- Remove Chinese accounts like this per feedback from Melia and Israel
        AND subscription_end_date >= '{{ end_date }}' -- filter to just active subscriptions per feedback by Melia
    GROUP BY dim_crm_account_id -- dim_crm_account_id is not unique at each snapshot date, hence the group by


--Snapshot for the set period prior to the "current" month (as specified by SNAPSHOT_DT).
), period_2 AS (

    SELECT dim_crm_account_id
        , COUNT(dim_subscription_id) AS num_of_subs_prev
        , MAX(parent_crm_account_tsp_account_employees) AS crm_account_tsp_account_employees_prev
        , SUM(mrr) AS sum_mrr_prev
        , SUM(arr) AS sum_arr_prev
        , SUM(quantity) AS license_count_prev
        , SUM(CASE WHEN product_delivery_type = 'Self-Managed' THEN 1 ELSE 0 END) AS self_managed_instance_count_prev
        , SUM(CASE WHEN product_delivery_type = 'SaaS' THEN 1 ELSE 0 END) AS saas_instance_count_prev
        , SUM(CASE WHEN product_delivery_type = 'Others' THEN 1 ELSE 0 END) AS others_instance_count_prev
        , MAX(CASE WHEN product_tier_name LIKE '%Ultimate%' THEN 1 ELSE 0 END) AS is_ultimate_product_tier_prev
        , MAX(CASE WHEN product_tier_name LIKE '%Premium%' THEN 1 ELSE 0 END) AS is_premium_product_tier_prev
        , MAX(CASE WHEN product_tier_name LIKE '%Starter%' or product_tier_name LIKE '%Bronze%' THEN 1 ELSE 0 END) AS is_starter_bronze_product_tier_prev
        , MAX(CASE WHEN service_type = 'Full Service' THEN 1 ELSE 0 END) AS is_service_type_full_service_prev
        , MAX(CASE WHEN service_type = 'Support Only' THEN 1 ELSE 0 END) AS is_service_type_support_only_prev
        , SUM(CASE WHEN subscription_status = 'Cancelled' OR (subscription_status = 'Active' AND subscription_end_date <= dateadd(MONTH, -3, '{{ end_date }}')) THEN 1 ELSE 0 END) AS cancelled_subs_prev --added 3 months before counting active subscriptions as cancelled per Israel's feedback
    FROM mart_arr_snapshot_bottom_up
    WHERE snapshot_date = '{{ end_date }}' -- limit to snapshot to day before our prediction window
        AND ARR_MONTH = DATE_TRUNC('MONTH', DATEADD('{{ period_type }}', -'{{ delta_value }}', cast('{{ end_date }}' as date))) -- limit to customer's data for just the PERIOD prior to where the '{{ end_date }}' falls
        AND is_jihu_account != 'TRUE' -- Remove Chinese accounts like this per feedback from Melia and Israel
    GROUP BY dim_crm_account_id


-- Any metrics you would want to calculate the require multiple ARR_MONTHS. Could be lifetime metrics, over the last year, future expected ARR. etc.
/*
), lifetime AS (

    SELECT dim_crm_account_id
           , COUNT(DISTINCT dim_subscription_id) AS num_of_subs_lifetime
           , AVG(DATEDIFF(month, subscription_start_month, subscription_end_month)) AS subscription_duration_in_months_lifetime
           --not added in period1 cte as it will always give 0
           , SUM(CASE WHEN subscription_status = 'Cancelled' THEN 1 ELSE 0 END) AS cancelled_subscriptions_lifetime
    FROM PROD.COMMON_MART_SALES.MART_ARR_SNAPSHOT_BOTTOM_UP -- Contains Snapshot for every date from 2020-03-01 to Present
    WHERE snapshot_date = $SNAPSHOT_DT -- limit to snapshot X periods prior to today
        AND is_jihu_account != 'TRUE' -- Remove Chinese accounts per Bhawana's notes, confirmed by Melia, removed missing values as per Israel's feedback
    GROUP BY dim_crm_account_id -- dim_crm_account_id is not unique at each snapshot date, hence the group by
*/


-- SFDC Opportunity Table as it appears on the date of '{{ end_date }}'
), opps AS (

   SELECT account_id
        , COUNT(DISTINCT opportunity_id) AS num_opportunities
        , SUM(CASE WHEN sales_path = 'Sales Assisted' THEN 1 ELSE 0 END) AS sales_path_sales_assisted_cnt
        , SUM(CASE WHEN sales_path = 'Web Direct' THEN 1 ELSE 0 END) AS sales_path_web_direct_cnt
        , SUM(CASE WHEN deal_size = 'Other' THEN 1 ELSE 0 END) AS deal_size_other_cnt
        , SUM(CASE WHEN deal_size = '1 - Small (<5k)' THEN 1 ELSE 0 END) AS deal_size_small_cnt
        , SUM(CASE WHEN deal_size = '2 - Medium (5k - 25k)' THEN 1 ELSE 0 END) AS deal_size_medium_cnt
        , SUM(CASE WHEN deal_size = '3 - Big (25k - 100k)' THEN 1 ELSE 0 END) AS deal_size_big_cnt
        , SUM(CASE WHEN deal_size = '4 - Jumbo (>100k)' THEN 1 ELSE 0 END) AS deal_size_jumbo_cnt
        , SUM(CASE WHEN  stage_name IN ('Closed Won') and order_type_stamped != '7. PS / Other' THEN 1 ELSE 0 END) AS won_opportunities
        , SUM(CASE WHEN  stage_name IN ('8-Closed Lost', 'Closed Lost') and order_type_stamped != '7. PS / Other' THEN 1 ELSE 0 END) AS lost_opportunities
        , COUNT(CASE WHEN order_type_stamped in ('2. New - Connected', '3. Growth') and net_arr > 0 and stage_name IN ('Closed Won') THEN 1 ELSE 0 END) AS num_expansions
        , COUNT(CASE WHEN order_type_stamped in ('4. Contraction', '5. Churn - Partial') and net_arr <=0 THEN 1 ELSE 0 END) AS num_contractions
        , SUM(CASE WHEN  sales_type = 'Renewal' THEN 1 ELSE 0 END) AS num_opportunities_by_renewal
        , SUM(CASE WHEN  order_type_stamped = '1. New - First Order' THEN 1 ELSE 0 END) AS num_opportunities_new_business
        , SUM(CASE WHEN  sales_type = 'Add-On Business' THEN 1 ELSE 0 END) AS num_opportunities_add_on_business
        , SUM(net_arr) AS sum_net_arr
        , SUM(CASE WHEN  stage_name IN ('Closed Won') THEN net_arr ELSE 0 END) AS sum_net_arr_won_opportunities
        , SUM(CASE WHEN  stage_name IN ('8-Closed Lost') THEN net_arr ELSE 0 END) AS sum_net_arr_lost_opportunities
        , SUM(CASE WHEN  sales_type = 'Renewal' and stage_name IN ('Closed Won') THEN 1 ELSE 0 END) AS won_opportunities_by_renewal
        , SUM(CASE WHEN  order_type_stamped = '1. New - First Order' and stage_name IN ('Closed Won') THEN 1 ELSE 0 END) AS won_opportunities_new_business
        , SUM(CASE WHEN  sales_type = 'Add-On Business' and stage_name IN ('Closed Won') THEN 1 ELSE 0 END) AS won_opportunities_add_on_business
        , SUM(CASE WHEN  sales_type = 'Renewal' and stage_name IN ('8-Closed Lost') THEN 1 ELSE 0 END) AS lost_opportunities_by_renewal
        , SUM(CASE WHEN  order_type_stamped = '1. New - First Order' and stage_name IN ('8-Closed Lost') THEN 1 ELSE 0 END) AS lost_opportunities_new_business
        , SUM(CASE WHEN  sales_type = 'Add-On Business' and stage_name IN ('8-Closed Lost') THEN 1 ELSE 0 END) AS lost_opportunities_add_on_business
        , MAX(competitors_other_flag) AS competitors_other
        , MAX(competitors_gitlab_core_flag) AS competitors_gitlab_core
        , MAX(competitors_none_flag) AS competitors_none
        , MAX(competitors_github_enterprise_flag) AS competitors_github_enterprise
        , MAX(competitors_bitbucket_server_flag) AS competitors_bitbucket_server
        , MAX(competitors_unknown_flag) AS competitors_unknown
        , MAX(competitors_github_flag) AS competitors_github
        , MAX(competitors_gitlab_flag) AS competitors_gitlab
        , MAX(competitors_jenkins_flag) AS competitors_jenkins
        , MAX(competitors_azure_devops_flag) AS competitors_azure_devops
        , MAX(competitors_svn_flag) AS competitors_svn
        , MAX(competitors_bitbucket_flag) AS competitors_bitbucket
        , MAX(competitors_atlassian_flag) AS competitors_atlassian
        , MAX(competitors_perforce_flag) AS competitors_perforce
        , MAX(competitors_visual_studio_flag) AS competitors_visual_studio
        , MAX(competitors_azure_flag) AS competitors_azure
        , MAX(competitors_amazon_code_commit_flag) AS competitors_amazon_code_commit
        , MAX(competitors_circleci_flag) AS competitors_circleci
        , MAX(competitors_bamboo_flag) AS competitors_bamboo
        , MAX(competitors_aws_flag) AS competitors_aws
        , SUM(CASE WHEN cp_use_cases = 'CI: Automate build and test' THEN 1 ELSE 0 END) AS use_case_continuous_integration
        , SUM(CASE WHEN cp_use_cases = 'DevSecOps: Test for application security vulnerabilities early and often' THEN 1 ELSE 0 END) AS use_case_dev_sec_ops
        , SUM(CASE WHEN cp_use_cases = 'CD: Automate delivery and deployment' THEN 1 ELSE 0 END) AS use_case_continuous_delivery
        , SUM(CASE WHEN cp_use_cases = 'VCC: Collaborate and manage source code (SCM), designs, and more' THEN 1 ELSE 0 END) AS use_case_version_controlled_configuration
        , SUM(CASE WHEN cp_use_cases = 'Simplify DevOps: Manage and streamline my entire DevOps lifecycle' THEN 1 ELSE 0 END) AS use_case_simplify_dev_ops
        , SUM(CASE WHEN cp_use_cases = 'Agile: Improve how we iteratively plan, manage, and deliver projects' THEN 1 ELSE 0 END) AS use_case_agile
        , SUM(CASE WHEN cp_use_cases = 'Other' THEN 1 ELSE 0 END) AS use_case_other
        , SUM(CASE WHEN cp_use_cases = 'Cloud-Native: Embrace modern, cloud-native application development' THEN 1 ELSE 0 END) AS use_case_cloud_native
        , SUM(CASE WHEN cp_use_cases = 'GitOps: Automatically provision, manage and maintain infrastructure' THEN 1 ELSE 0 END) AS use_case_git_ops
    FROM {{ref('wk_sales_sfdc_opportunity_snapshot_history_xf')}}
    WHERE snapshot_date = '{{ end_date }}'
        AND opportunity_category IN ('Standard', 'Decommissioned', 'Ramp Deal') -- filter as requested by Noel
        AND created_date BETWEEN DATEADD('{{ period_type }}', -'{{ delta_value }}', '{{ end_date }}') AND '{{ end_date }}'
    GROUP BY account_id

), events_salesforce AS (

    SELECT
       account_id as account_id
     , SUM(CASE WHEN event_type = 'IQM' THEN 1 ELSE 0 END) AS initial_qualifying_meeting_event_count
     , SUM(CASE WHEN event_type = 'Meeting' THEN 1 ELSE 0 END) AS meeting_event_count
     , SUM(CASE WHEN event_type = 'Web Conference' THEN 1 ELSE 0 END) AS web_conference_event_count
     , SUM(CASE WHEN event_type = 'Call' THEN 1 ELSE 0 END) AS call_event_count
     , SUM(CASE WHEN event_type = 'Demo' THEN 1 ELSE 0 END) AS demo_event_count
     , SUM(CASE WHEN event_type = 'In Person' THEN 1 ELSE 0 END) AS in_person_event_count
     , SUM(CASE WHEN event_type = 'Renewal' THEN 1 ELSE 0 END) AS renewal_event_count
     , SUM(CASE WHEN event_type IS NOT NULL THEN 1 ELSE 0 END) AS total_event_count
    FROM {{ref('sfdc_event_source')}}
    WHERE created_at BETWEEN DATEADD('{{ period_type }}', -'{{ delta_value }}', '{{ end_date }}') AND '{{ end_date }}'  --filter PERIOD window. Because no histroic event table, going off createddate
    GROUP BY account_id

), tasks_salesforce AS (

    SELECT
       accountid as account_id
     , SUM(CASE WHEN type = 'Email' THEN 1 ELSE 0 END) AS email_task_count
     , SUM(CASE WHEN type = 'Call' THEN 1 ELSE 0 END) AS call_task_count
     , SUM(CASE WHEN type = 'Demo' THEN 1 ELSE 0 END) AS demo_task_count
     , SUM(CASE WHEN type = 'Sales Alignment' THEN 1 ELSE 0 END) AS sales_alignment_task_count
     , SUM(CASE WHEN type IS NOT NULL THEN 1 ELSE 0 END) as total_task_count
     , SUM(is_answered__c) as is_answered_task
     , SUM(is_busy__c) AS is_busy_task
     , SUM(is_correct_contact__c) AS is_correct_contact_task
     , SUM(is_left_message__c) AS is_left_message_task
     , SUM(is_not_answered__c) AS is_not_answered_task
    FROM {{ source('salesforce', 'task') }}
    WHERE createddate BETWEEN DATEADD('{{ period_type }}', -'{{ delta_value }}', '{{ end_date }}') AND '{{ end_date }}'  --filter PERIOD window. Because no histroic task table, going on createddate
    GROUP BY account_id

), zi_technologies AS (

    SELECT account_id_18__c AS account_id
        , MAX(zi_revenue__c) AS zi_revenue
        , MAX(zi_industry__c) AS zi_industry
        , MAX(zi_sic_code__c) AS zi_sic_code
        , MAX(zi_naics_code__c) AS zi_naics_code
        , MAX(zi_number_of_developers__c) AS zi_developers_cnt
        --, MAX(zi_products_and_services__c) AS zi_products_and_services -- Leaving out for now but could be useful to parse later

        --Atlassian
        , MAX(CASE WHEN CONTAINS(zi_technologies__c, 'ARE_USED: Atlassian') THEN 1 END) AS zi_atlassian_flag
        , MAX(CASE WHEN (CONTAINS(zi_technologies__c, 'ARE_USED: BitBucket') OR CONTAINS(zi_technologies__c, 'ARE_USED: AtlASsian Bitbucket')) THEN 1 END) AS zi_bitbucket_flag
        , MAX(CASE WHEN CONTAINS(zi_technologies__c, 'ARE_USED: Atlassian Jira Agile Tools') THEN 1 END) AS zi_jira_flag

        --GCP
        , MAX(CASE WHEN (CONTAINS(zi_technologies__c, 'ARE_USED: Google Cloud Platform') OR CONTAINS(zi_technologies__c, 'ARE_USED: GCP')) THEN 1 END) AS zi_gcp_flag

        --Github
        , MAX(CASE WHEN CONTAINS(zi_technologies__c, 'ARE_USED: GitHub') THEN 1 END) AS zi_github_flag
        , MAX(CASE WHEN CONTAINS(zi_technologies__c, 'ARE_USED: GitHub Enterprise') THEN 1 END) AS zi_github_enterprise_flag

        --AWS
        , MAX(CASE WHEN CONTAINS(zi_technologies__c, 'ARE_USED: AWS') THEN 1 END) AS zi_aws_flag
        , MAX(CASE WHEN (CONTAINS(zi_technologies__c, 'ARE_USED: Amazon AWS Identity and Access Management') OR CONTAINS(zi_technologies__c, 'Amazon AWS Identity and Access Management (IAM)')) THEN 1 END) AS zi_aws_iam_flag
        , MAX(CASE WHEN CONTAINS(zi_technologies__c, 'ARE_USED: Amazon AWS CloudTrail') THEN 1 END) AS zi_aws_cloud_trail_flag

        --Other CI
        , MAX(CASE WHEN CONTAINS(zi_technologies__c, 'ARE_USED: Hashicorp') THEN 1 END) AS zi_hAShicorp_flag
        , MAX(CASE WHEN (CONTAINS(zi_technologies__c, 'ARE_USED: CircleCI') OR CONTAINS(zi_technologies__c, 'ARE_USED: Circle Internet Services')) THEN 1 END) AS zi_circleci_flag
        , MAX(CASE WHEN CONTAINS(zi_technologies__c, 'ARE_USED: TravisCI') THEN 1 END) AS zi_travisci_flag
        --Open Source/Free
        , MAX(CASE WHEN (CONTAINS(zi_technologies__c, 'ARE_USED: Apache Subversion') OR CONTAINS(zi_technologies__c, 'ARE_USED: SVN')) THEN 1 END) AS zi_apache_subversion_flag
        , MAX(CASE WHEN CONTAINS(zi_technologies__c, 'ARE_USED: Jenkins') THEN 1 END) AS zi_jenkins_flag
        , MAX(CASE WHEN CONTAINS(zi_technologies__c, 'ARE_USED: TortoiseSVN') THEN 1 END) AS zi_tortoise_svn_flag
        , MAX(CASE WHEN CONTAINS(zi_technologies__c, 'ARE_USED: Kubernetes') THEN 1 END) AS zi_kubernetes_flag
    FROM {{ source('snapshots', 'sfdc_account_snapshots') }}
    WHERE CAST(DBT_UPDATED_AT AS date) = '{{ end_date }}' -- Cast from datetime to date
    GROUP BY account_id  -- snapshots occur multiple times a day so data is not unique at the acccount + dbt_updated_at level.


), bizible AS (

    SELECT
        dim_crm_account_id
        , COUNT(dim_crm_touchpoint_id) AS num_bizible_touchpoints
        , COUNT(DISTINCT dim_campaign_id) AS num_campaigns
        , SUM(CASE WHEN bizible_touchpoint_source = 'Web Direct' THEN 1 ELSE 0 END) AS touchpoint_source_web_direct
        , SUM(CASE WHEN bizible_touchpoint_source = 'Organic - Google' THEN 1 ELSE 0 END) AS touchpoint_source_web_organic_google
        , SUM(CASE WHEN bizible_touchpoint_source = 'CRM Campaign' THEN 1 ELSE 0 END) AS touchpoint_source_crm_campaign
        , SUM(CASE WHEN bizible_touchpoint_source = 'marketo' THEN 1 ELSE 0 END) AS touchpoint_source_marketo
        , SUM(CASE WHEN bizible_touchpoint_source = 'CRM Activity' THEN 1 ELSE 0 END) AS touchpoint_source_crm_activity
        , SUM(CASE WHEN bizible_touchpoint_source in ('facebook', 'Facebook', 'facebook.com', 'linkedin', 'LinkedIn', 'linkedin_elevate', 'linkedin/', 'twitter', 'Twitter', 'twitter.com') THEN 1 ELSE 0 END) AS touchpoint_source_social_media
        , SUM(CASE WHEN bizible_touchpoint_type = 'Web Form' THEN 1 ELSE 0 END) AS touchpoint_type_web_form
        , SUM(CASE WHEN bizible_touchpoint_type = 'Web Visit' THEN 1 ELSE 0 END) AS touchpoint_type_web_visit
        , SUM(CASE WHEN bizible_touchpoint_type = 'CRM' THEN 1 ELSE 0 END) AS touchpoint_type_crm
        , SUM(CASE WHEN bizible_touchpoint_type = 'IQM' THEN 1 ELSE 0 END) AS touchpoint_type_iqm
        , SUM(CASE WHEN bizible_touchpoint_type = 'Web Chat' THEN 1 ELSE 0 END) AS touchpoint_type_web_chat
        , SUM(CASE WHEN bizible_marketing_channel = 'Direct' THEN 1 ELSE 0 END) AS touchpoint_marketing_channel_direct
        , SUM(CASE WHEN bizible_marketing_channel = 'Organic Search' THEN 1 ELSE 0 END) AS touchpoint_marketing_channel_organic_search
        , SUM(CASE WHEN bizible_marketing_channel = 'Email' THEN 1 ELSE 0 END) AS touchpoint_marketing_channel_email
        , SUM(CASE WHEN bizible_marketing_channel = 'Web Referral' THEN 1 ELSE 0 END) AS touchpoint_marketing_channel_web_referral
        , SUM(CASE WHEN bizible_marketing_channel = 'Event' THEN 1 ELSE 0 END) AS touchpoint_marketing_channel_web_event
        , SUM(CASE WHEN type = 'Inbound Request' THEN 1 ELSE 0 END) AS touchpoint_type_inbound_request
        , SUM(CASE WHEN type = 'Direct Mail' THEN 1 ELSE 0 END) AS touchpoint_type_direct_mail
        , SUM(CASE WHEN type = 'Trial' THEN 1 ELSE 0 END) AS touchpoint_type_trial
        , SUM(CASE WHEN type = 'Webcast' THEN 1 ELSE 0 END) AS touchpoint_type_webcast
        , SUM(CASE WHEN bizible_medium = 'Web' THEN 1 ELSE 0 END) AS touchpoint_bizible_medium_web
        , SUM(CASE WHEN bizible_medium = 'Search' THEN 1 ELSE 0 END) AS touchpoint_bizible_medium_search
        , SUM(CASE WHEN bizible_medium = 'email' THEN 1 ELSE 0 END) AS touchpoint_bizible_medium_email
        , SUM(CASE WHEN bizible_medium = 'Trial' THEN 1 ELSE 0 END) AS touchpoint_bizible_medium_trial
        , SUM(CASE WHEN bizible_medium = 'Webcast' THEN 1 ELSE 0 END) AS touchpoint_bizible_medium_webcast
        , SUM(CASE WHEN crm_person_status = 'Qualified' THEN 1 ELSE 0 END) AS touchpoint_crm_person_status_qualified
        , SUM(CASE WHEN crm_person_status = 'Inquery' THEN 1 ELSE 0 END) AS touchpoint_crm_person_status_inquery
        , SUM(CASE WHEN crm_person_status = 'MQL' THEN 1 ELSE 0 END) AS touchpoint_crm_person_status_mql
        , SUM(CASE WHEN crm_person_status = 'Nurture' THEN 1 ELSE 0 END) AS touchpoint_crm_person_status_nurture
        , SUM(CASE WHEN crm_person_status = 'Qualifying' THEN 1 ELSE 0 END) AS touchpoint_crm_person_status_qualifying
        , SUM(CASE WHEN crm_person_status = 'Accepted' THEN 1 ELSE 0 END) AS touchpoint_crm_person_status_accepted
        , SUM(CASE WHEN crm_person_title in ('CTO', 'Chief Technology Officer') THEN 1 ELSE 0 END) AS touchpoint_crm_person_title_cto
        , SUM(CASE WHEN crm_person_title = 'Software Developer' THEN 1 ELSE 0 END) AS touchpoint_crm_person_title_software_developer
        , SUM(CASE WHEN crm_person_title = 'Software Engineer' THEN 1 ELSE 0 END) AS touchpoint_crm_person_title_software_engineer
        , SUM(CASE WHEN crm_person_title = 'Development Team Lead' THEN 1 ELSE 0 END) AS touchpoint_crm_person_title_software_dev_team_lead
    FROM {{ref('mart_crm_attribution_touchpoint')}}
    WHERE bizible_touchpoint_date BETWEEN DATEADD('{{ period_type }}', -'{{ delta_value }}', '{{ end_date }}') AND '{{ end_date }}'
    GROUP BY dim_crm_account_id

), product_usage AS (

    SELECT
        dim_crm_account_id
        , AVG(umau_28_days_user) AS unique_active_user
        , AVG(action_monthly_active_users_project_repo_28_days_user) AS action_monthly_active_users_project_repo_avg
        , AVG(merge_requests_28_days_user) AS merge_requests_avg
        , AVG(projects_with_repositories_enabled_28_days_user) AS projects_with_repositories_enabled_avg
        , AVG(ci_pipelines_28_days_user) AS ci_pipelines_avg
        , AVG(ci_internal_pipelines_28_days_user) AS ci_internal_pipelines_avg
        , AVG(ci_builds_28_days_user) AS ci_builds_avg
        , AVG(ci_pipeline_config_repository_28_days_user) AS ci_pipeline_config_repository_avg
        , AVG(user_unique_users_all_secure_scanners_28_days_user) AS user_unique_users_all_secure_scanners_avg
        , AVG(user_sast_jobs_28_days_user) AS user_sast_jobs_avg
        , AVG(user_dast_jobs_28_days_user) AS user_dast_jobs_avg
        , AVG(user_dependency_scanning_jobs_28_days_user) AS user_dependency_scanning_jobs_avg
        , AVG(user_license_management_jobs_28_days_user) AS user_license_management_jobs_avg
        , AVG(user_secret_detection_jobs_28_days_user) AS user_secret_detection_jobs_avg
        , AVG(user_container_scanning_jobs_28_days_user) AS user_container_scanning_jobs_avg
        , AVG(deployments_28_days_user) as deployments_avg
        , AVG(releases_28_days_user) as releases_avg
        , AVG(epics_28_days_user) as epics_avg
        , AVG(issues_28_days_user) as issues_avg
        , AVG(analytics_28_days_user) as analytics_avg
        , SUM(CASE WHEN instance_type='Production' THEN active_user_count END) AS active_user_count_cnt
        , SUM(CASE WHEN instance_type='Production' THEN license_user_count END) AS license_user_count_cnt
        , SUM(CASE WHEN instance_type='Production' THEN billable_user_count END) AS billable_user_count_cnt
        , MAX(commit_comment_all_time_event) AS commit_comment_all_time_event
        , MAX(source_code_pushes_all_time_event) AS source_code_pushes_all_time_event
        , MAX(template_repositories_all_time_event) AS template_repositories_all_time_event
        , MAX(ci_runners_all_time_event) AS ci_runners_all_time_event
        , MAX(auto_devops_enabled_all_time_event) AS auto_devops_enabled_all_time_event
        , MAX(projects_with_packages_all_time_event) as projects_with_packages_all_time_event
        , MAX(merge_requests_all_time_event) as merge_requests_all_time_event
        , MAX(epics_all_time_event) as epics_all_time_event
        , MAX(issues_all_time_event) as issues_all_time_event
        , MAX(projects_all_time_event) as projects_all_time_event
        , MAX(CASE WHEN gitlab_shared_runners_enabled = 'TRUE' THEN 1 ELSE 0 END) AS gitlab_shared_runners_enabled
        , MAX(CASE WHEN container_registry_enabled = 'TRUE' THEN 1 ELSE 0 END) AS container_registry_enabled
        , MAX(CASE WHEN instance_type='Production' THEN max_historical_user_count END) AS max_historical_user_count
        , MAX(CAST(SUBSTRING(cleaned_version,0,CHARINDEX('.',cleaned_version)-1) AS INT)) as gitlab_version
    FROM {{ref('mart_product_usage_paid_user_metrics_monthly')}}
    WHERE PING_CREATED_AT IS NOT NULL
        AND SNAPSHOT_MONTH BETWEEN DATE_TRUNC(MONTH, DATEADD(MONTH, -'{{ delta_value }}', cast('{{ end_date }}' as date))) AND cast('{{ end_date }}' as date)
    GROUP BY dim_crm_account_id

)

-- This is the final output table that creates the modeling dataset
SELECT
    p1.dim_crm_account_id AS crm_account_id

    --Zuora Fields
    , p1.num_of_subs AS subs_cnt
    , p1.cancelled_subs AS cancelled_subs_cnt
    , CASE WHEN p1.crm_account_tsp_region = 'AMER' OR p1.crm_account_tsp_region LIKE 'AMER%' OR p1.crm_account_tsp_region LIKE 'US%' THEN 'AMER'
           WHEN p1.crm_account_tsp_region = 'EMEA' OR p1.crm_account_tsp_region LIKE 'Germany%' THEN 'EMEA'
           WHEN p1.crm_account_tsp_region IS NULL THEN 'Unknown'
           ELSE p1.crm_account_tsp_region END AS account_region
    --, COALESCE(p1.crm_account_tsp_region, 'Unknown') AS account_region
    , COALESCE(p1.parent_crm_account_sales_segment, 'Unknown') AS account_sales_segment
    , COALESCE(p1.parent_crm_account_industry, 'Unknown') AS account_industry
    , COALESCE(p1.parent_crm_account_billing_country, 'Unknown') AS account_billing_country
    , COALESCE(p1.parent_crm_account_owner_team, 'Unknown') AS account_owner_team
    , COALESCE(p1.parent_crm_account_sales_territory, 'Unknown') AS account_sales_territory
    , CASE WHEN p1.parent_crm_account_tsp_region = 'AMER' OR p1.parent_crm_account_tsp_region LIKE 'AMER%' OR p1.parent_crm_account_tsp_region LIKE 'US%' THEN 'AMER'
           WHEN p1.parent_crm_account_tsp_region = 'EMEA' OR p1.parent_crm_account_tsp_region LIKE 'Germany%' THEN 'EMEA'
           WHEN p1.parent_crm_account_tsp_region IS NULL THEN 'Unknown'
           ELSE p1.parent_crm_account_tsp_region END AS parent_account_region
    --, COALESCE(p1.parent_crm_account_tsp_region, 'Unknown') AS parent_account_region
    , COALESCE(p1.parent_crm_account_tsp_sub_region, 'Unknown') AS parent_account_sub_region
    , COALESCE(p1.parent_crm_account_tsp_area, 'Unknown') AS parent_account_area
    , p1.crm_account_tsp_account_employees AS parent_account_employees_cnt
    , p1.parent_crm_account_tsp_max_family_employees AS parent_account_max_family_employees_cnt
    , p1.parent_crm_account_employee_count_band AS parent_account_employee_count_band
    , p1.is_ultimate_product_tier AS is_ultimate_product_tier_flag
    , p1.is_premium_product_tier AS is_premium_product_tier_flag
    , p1.is_starter_bronze_product_tier AS is_starter_bronze_product_tier_flag
    , p1.is_service_type_full_service AS is_service_type_full_service_flag
    , p1.is_service_type_support_only AS is_service_type_support_only_flag
    , p1.subscription_months_into AS subscription_months_into
    , p1.subscription_months_remaining AS subscription_months_remaining
    , p1.subscription_duration_in_months AS subscription_duration_in_months
    , p1.account_tenure_in_months AS account_tenure_in_months
    , p1.health_number AS health_number
    , COALESCE(p1.sum_mrr, 0) AS mrr_amt
    , COALESCE(p1.sum_arr, 0) AS arr_amt
    , COALESCE(p1.license_count, 0) AS license_cnt
    , p1.sum_arr / p1.license_count AS arpu
    , COALESCE(p1.self_managed_instance_count, 0) AS self_managed_instance_cnt
    , COALESCE(p1.saas_instance_count, 0)   AS saas_instance_cnt
    , COALESCE(p1.others_instance_count, 0) AS others_instance_cnt
    , COALESCE(p1.num_products_purchased, 0) AS products_purchased_cnt

    --Previous Period Zuora Fields
    , COALESCE(p2.sum_arr_prev, 0) AS arr_prev_amt
    , COALESCE(p2.sum_mrr_prev, 0) AS mrr_prev_amt
    , COALESCE(p2.cancelled_subs_prev, 0) AS cancelled_subs_prev_cnt
    , COALESCE(p2.num_of_subs_prev, 0) AS subs_prev_cnt
    , COALESCE(crm_account_tsp_account_employees_prev, 0) AS crm_account_tsp_account_employees_prev_cnt
    , COALESCE(license_count_prev, 0) AS license_prev_cnt

    --Zuora Change Fields
    , CASE WHEN sum_arr_prev > 0 THEN (sum_arr - sum_arr_prev) / sum_arr_prev ELSE 1 END AS arr_change_pct
    , COALESCE(sum_arr, 0) - COALESCE(sum_arr_prev, 0) AS sum_arr_change_amt
    , CASE WHEN sum_mrr_prev > 0 THEN (sum_mrr - sum_mrr_prev) / sum_mrr_prev ELSE 1 END AS mrr_change_pct
    , COALESCE(sum_mrr, 0) - COALESCE(sum_mrr_prev, 0) AS mrr_change_amt
    , CASE WHEN crm_account_tsp_account_employees_prev > 0 THEN (crm_account_tsp_account_employees - crm_account_tsp_account_employees_prev) / crm_account_tsp_account_employees_prev ELSE 1 END AS crm_account_tsp_account_employees_change_pct
    , COALESCE(crm_account_tsp_account_employees, 0) - COALESCE(crm_account_tsp_account_employees_prev, 0) AS crm_account_tsp_account_employees_change_cnt
    , CASE WHEN num_of_subs_prev > 0 THEN (num_of_subs - num_of_subs_prev) / num_of_subs_prev ELSE 1 END AS subs_change_pct
    , COALESCE(num_of_subs, 0) - COALESCE(num_of_subs_prev, 0) AS subs_change_cnt
    , CASE WHEN license_count_prev > 0 THEN (license_count - license_count_prev) / license_count_prev ELSE 1 END AS license_change_pct
    , COALESCE(license_count, 0) - COALESCE(license_count_prev, 0) AS license_change_cnt
    , CASE WHEN cancelled_subs_prev > 0 THEN (cancelled_subs - cancelled_subs_prev) / cancelled_subs_prev ELSE 1 END AS cancelled_subs_change_pct
    , COALESCE(cancelled_subs, 0) - COALESCE(cancelled_subs_prev, 0) AS cancelled_subs_change_cnt
    , COALESCE(p1.self_managed_instance_count, 0) - COALESCE(p2.self_managed_instance_count_prev, 0) AS self_managed_instance_change_cnt
    , COALESCE(p1.saas_instance_count, 0) - COALESCE(p2.saas_instance_count_prev, 0) AS saas_instance_change_cnt
    , COALESCE(p1.others_instance_count, 0) - COALESCE(p2.others_instance_count_prev, 0) AS others_instance_change_cnt
    , COALESCE(p1.is_ultimate_product_tier, 0) - COALESCE(p2.is_ultimate_product_tier_prev, 0) AS ultimate_product_tier_change_cnt
    , COALESCE(p1.is_premium_product_tier, 0) - COALESCE(p2.is_premium_product_tier_prev, 0) AS premium_product_tier_change_cnt
    , COALESCE(p1.is_starter_bronze_product_tier, 0) - COALESCE(p2.is_starter_bronze_product_tier_prev, 0) AS starter_bronze_product_tier_change_cnt
    , COALESCE(p1.is_service_type_full_service, 0) - COALESCE(p2.is_service_type_full_service_prev, 0) AS service_type_full_service_change_cnt
    , COALESCE(p1.is_service_type_support_only, 0) - COALESCE(p2.is_service_type_support_only_prev, 0) AS service_type_support_only_change_cnt

    --Salesforce Opportunity Fields
    , COALESCE(o.num_opportunities, 0) AS opportunities_cnt
    , COALESCE(o.sales_path_sales_assisted_cnt, 0) AS sales_path_sales_assisted_cnt
    , COALESCE(o.sales_path_web_direct_cnt, 0) AS sales_path_web_direct_cnt
    , COALESCE(o.deal_size_other_cnt, 0) AS deal_size_other_cnt
    , COALESCE(o.deal_size_small_cnt, 0) AS deal_size_small_cnt
    , COALESCE(o.deal_size_medium_cnt, 0) AS deal_size_medium_cnt
    , COALESCE(o.deal_size_big_cnt, 0) AS deal_size_big_cnt
    , COALESCE(o.deal_size_jumbo_cnt, 0) AS deal_size_jumbo_cnt
    , COALESCE(o.won_opportunities, 0) AS won_opportunities_cnt
    , COALESCE(o.lost_opportunities, 0) AS lost_opportunities_cnt
    , COALESCE(o.num_expansions, 0) AS expansions_cnt
    , COALESCE(o.num_contractions, 0) AS contractions_cnt
    , COALESCE(o.num_opportunities_by_renewal, 0) AS opportunities_by_renewal_cnt
    , COALESCE(o.num_opportunities_new_business, 0) AS opportunities_new_business_cnt
    , COALESCE(o.num_opportunities_add_on_business, 0) AS opportunities_add_on_business_cnt
    , COALESCE(o.sum_net_arr, 0) AS net_arr_amt
    , COALESCE(o.sum_net_arr_won_opportunities, 0) AS net_arr_won_opportunities_amt
    , COALESCE(o.sum_net_arr_lost_opportunities, 0) AS net_arr_lost_opportunities_amt
    , COALESCE(o.won_opportunities_by_renewal, 0) AS won_opportunities_by_renewal_cnt
    , COALESCE(o.won_opportunities_new_business, 0) AS won_opportunities_new_business_cnt
    , COALESCE(o.won_opportunities_add_on_business, 0) AS won_opportunities_add_on_business_cnt
    , COALESCE(o.lost_opportunities_by_renewal, 0) AS lost_opportunities_by_renewal_cnt
    , COALESCE(o.lost_opportunities_new_business, 0) AS lost_opportunities_new_business_cnt
    , COALESCE(o.lost_opportunities_add_on_business, 0) AS lost_opportunities_add_on_business_cnt

    , COALESCE(o.competitors_other, 0) AS competitors_other_flag
    , COALESCE(o.competitors_gitlab_core, 0) AS competitors_gitlab_core_flag
    , COALESCE(o.competitors_none, 0) AS competitors_none_flag
    , COALESCE(o.competitors_github_enterprise, 0) AS competitors_github_enterprise_flag
    , COALESCE(o.competitors_bitbucket_server, 0) AS competitors_bitbucket_server_flag
    , COALESCE(o.competitors_unknown, 0) AS competitors_unknown_flag
    , COALESCE(o.competitors_github, 0) AS competitors_github_flag
    , COALESCE(o.competitors_gitlab, 0) AS competitors_gitlab_flag
    , COALESCE(o.competitors_jenkins, 0) AS competitors_jenkins_flag
    , COALESCE(o.competitors_azure_devops, 0) AS competitors_azure_devops_flag
    , COALESCE(o.competitors_svn, 0) AS competitors_svn_flag
    , COALESCE(o.competitors_bitbucket, 0) AS competitors_bitbucket_flag
    , COALESCE(o.competitors_atlassian, 0) AS competitors_atlassian_flag
    , COALESCE(o.competitors_perforce, 0) AS competitors_perforce_flag
    , COALESCE(o.competitors_visual_studio, 0) AS competitors_visual_studio_flag
    , COALESCE(o.competitors_azure, 0) AS competitors_azure_flag
    , COALESCE(o.competitors_amazon_code_commit, 0) AS competitors_amazon_code_commit_flag
    , COALESCE(o.competitors_circleci, 0) AS competitors_circleci_flag
    , COALESCE(o.competitors_bamboo, 0) AS competitors_bamboo_flag
    , COALESCE(o.competitors_aws, 0) AS competitors_aws_flag

    , COALESCE(o.use_case_continuous_integration, 0) AS use_case_continuous_integration_cnt
    , COALESCE(o.use_case_dev_sec_ops, 0) AS use_case_dev_sec_ops_cnt
    , COALESCE(o.use_case_continuous_delivery, 0) AS use_case_continuous_delivery_cnt
    , COALESCE(o.use_case_version_controlled_configuration, 0) AS use_case_version_controlled_configuration_cnt
    , COALESCE(o.use_case_simplify_dev_ops, 0) AS use_case_simplify_dev_ops_cnt
    , COALESCE(o.use_case_agile, 0) AS use_case_agile_cnt
    , COALESCE(o.use_case_other, 0) AS use_case_other_cnt
    , COALESCE(o.use_case_cloud_native, 0) AS use_case_cloud_native_cnt
    , COALESCE(o.use_case_git_ops, 0) AS use_case_git_ops_cnt
    , CASE WHEN o.account_id IS NOT NULL THEN 1 ELSE 0 END AS has_sfdc_opportunities_flag

--ZoomInfo Fields
    , zt.zi_revenue AS zi_revenue
    , zt.zi_industry AS zi_industry
    , zt.zi_sic_code AS zi_sic_code
    , zt.zi_naics_code AS zi_naics_code
    , zt.zi_developers_cnt AS zi_developers_cnt
    , COALESCE(zt.zi_atlassian_flag, 0) AS zi_atlassian_flag
    , COALESCE(zt.zi_bitbucket_flag, 0) AS zi_bitbucket_flag
    , COALESCE(zt.zi_jira_flag, 0) AS zi_jira_flag
    , COALESCE(zt.zi_gcp_flag, 0) AS zi_gcp_flag
    , COALESCE(zt.zi_github_flag, 0) AS zi_github_flag
    , COALESCE(zt.zi_github_enterprise_flag, 0) AS zi_github_enterprise_flag
    , COALESCE(zt.zi_aws_flag, 0) AS zi_aws_flag
    , COALESCE(zt.zi_aws_iam_flag, 0) AS zi_aws_iam_flag
    , COALESCE(zt.zi_aws_cloud_trail_flag, 0) AS zi_aws_cloud_trail_flag
    , COALESCE(zt.zi_hashicorp_flag, 0) AS zi_hashicorp_flag
    , COALESCE(zt.zi_circleci_flag, 0) AS zi_circleci_flag
    , COALESCE(zt.zi_travisci_flag, 0) AS zi_travisci_flag
    , COALESCE(zt.zi_apache_subversion_flag, 0) AS zi_apache_subversion_flag
    , COALESCE(zt.zi_jenkins_flag, 0) AS zi_jenkins_flag
    , COALESCE(zt.zi_tortoise_svn_flag, 0) AS zi_tortoise_svn_flag
    , COALESCE(zt.zi_kubernetes_flag, 0) AS zi_kubernetes_flag
    , COALESCE(zt.zi_atlassian_flag, zt.zi_bitbucket_flag, zt.zi_jira_flag, 0) AS zi_atlassian_any_flag
    , COALESCE(zt.zi_github_flag, zt.zi_github_enterprise_flag, 0) AS zi_github_any_flag
    , COALESCE(zt.zi_aws_flag, zt.zi_aws_iam_flag, zt.zi_aws_cloud_trail_flag, 0) AS zi_aws_any_flag
    , COALESCE(zt.zi_hashicorp_flag, zt.zi_bitbucket_flag, zt.zi_jira_flag, 0) AS zi_other_ci_any_flag
    , COALESCE(zt.zi_apache_subversion_flag, zt.zi_jenkins_flag, zt.zi_tortoise_svn_flag, zt.zi_kubernetes_flag, 0) AS zi_open_source_any_flag

--Event Salesforce
    , COALESCE(es.initial_qualifying_meeting_event_count, 0) AS initial_qualifying_meeting_event_cnt
    , COALESCE(es.meeting_event_count, 0) AS meeting_event_cnt
    , COALESCE(es.web_conference_event_count, 0) AS web_conference_event_cnt
    , COALESCE(es.call_event_count, 0) AS call_event_cnt
    , COALESCE(es.demo_event_count, 0) AS demo_event_cnt
    , COALESCE(es.in_person_event_count, 0) AS in_person_event_cnt
    , COALESCE(es.renewal_event_count, 0) AS renewal_event_cnt
    , COALESCE(es.total_event_count, 0) AS total_event_cnt
    , CASE WHEN es.account_id IS NOT NULL THEN 1 ELSE 0 END AS has_sfdc_events_flag


--Task Salesforce
    , COALESCE(ts.email_task_count, 0) AS email_task_cnt
    , COALESCE(ts.call_task_count, 0) AS call_task_cnt
    , COALESCE(ts.demo_task_count, 0) AS demo_task_cnt
    , COALESCE(ts.sales_alignment_task_count, 0) AS sales_alignment_task_cnt
    , COALESCE(ts.total_task_count, 0) AS total_task_cnt
    , COALESCE(ts.is_answered_task, 0) AS is_answered_task_flag
    , COALESCE(ts.is_busy_task, 0) AS is_busy_task_flag
    , COALESCE(ts.is_correct_contact_task, 0) AS is_correct_contact_task_flag
    , COALESCE(ts.is_left_message_task, 0) AS is_left_message_task_flag
    , COALESCE(ts.is_not_answered_task, 0) AS is_not_answered_task_flag
    , CASE WHEN ts.account_id IS NOT NULL THEN 1 ELSE 0 END AS has_sfdc_tasks_flag

--Bizible Fields
    , COALESCE(b.num_bizible_touchpoints, 0) AS bizible_touchpoints_cnt
    , COALESCE(b.num_campaigns, 0) AS campaigns_cnt
    , COALESCE(b.touchpoint_source_web_direct, 0) AS touchpoint_source_web_direct_cnt
    , COALESCE(b.touchpoint_source_web_organic_google, 0) AS touchpoint_source_web_organic_google_cnt
    , COALESCE(b.touchpoint_source_crm_campaign, 0) AS touchpoint_source_crm_campaign_cnt
    , COALESCE(b.touchpoint_source_marketo, 0) AS touchpoint_source_marketo_cnt
    , COALESCE(b.touchpoint_source_crm_activity, 0) AS touchpoint_source_crm_activity_cnt
    , COALESCE(b.touchpoint_source_social_media, 0) AS touchpoint_source_social_media_cnt
    , COALESCE(b.touchpoint_type_web_form, 0) AS touchpoint_type_web_form_cnt
    , COALESCE(b.touchpoint_type_web_visit, 0) AS touchpoint_type_web_visit_cnt
    , COALESCE(b.touchpoint_type_crm , 0)AS touchpoint_type_crm_cnt
    , COALESCE(b.touchpoint_type_iqm, 0) AS touchpoint_type_iqm_cnt
    , COALESCE(b.touchpoint_type_web_chat, 0) AS touchpoint_type_web_chat_cnt
    , COALESCE(b.touchpoint_marketing_channel_direct, 0) AS touchpoint_marketing_channel_direct_cnt
    , COALESCE(b.touchpoint_marketing_channel_organic_search, 0) AS touchpoint_marketing_channel_organic_search_cnt
    , COALESCE(b.touchpoint_marketing_channel_email, 0) AS touchpoint_marketing_channel_email_cnt
    , COALESCE(b.touchpoint_marketing_channel_web_referral, 0) AS touchpoint_marketing_channel_web_referral_cnt
    , COALESCE(b.touchpoint_marketing_channel_web_event, 0)  AS touchpoint_marketing_channel_web_event_cnt
    , COALESCE(b.touchpoint_type_inbound_request, 0) AS touchpoint_type_inbound_request_cnt
    , COALESCE(b.touchpoint_type_direct_mail, 0) AS touchpoint_type_direct_mail_cnt
    , COALESCE(b.touchpoint_type_trial, 0) AS touchpoint_type_trial_cnt
    , COALESCE(b.touchpoint_type_webcast, 0) AS touchpoint_type_webcast_cnt
    , COALESCE(b.touchpoint_bizible_medium_web, 0) AS touchpoint_bizible_medium_web_cnt
    , COALESCE(b.touchpoint_bizible_medium_search, 0) AS touchpoint_bizible_medium_search_cnt
    , COALESCE(b.touchpoint_bizible_medium_email, 0) AS touchpoint_bizible_medium_email_cnt
    , COALESCE(b.touchpoint_bizible_medium_trial, 0) AS touchpoint_bizible_medium_trial_cnt
    , COALESCE(b.touchpoint_bizible_medium_webcast, 0) AS touchpoint_bizible_medium_webcast_cnt
    , COALESCE(b.touchpoint_crm_person_status_qualified, 0) AS touchpoint_crm_person_status_qualified_cnt
    , COALESCE(b.touchpoint_crm_person_status_inquery, 0) AS touchpoint_crm_person_status_inquery_cnt
    , COALESCE(b.touchpoint_crm_person_status_mql, 0) AS touchpoint_crm_person_status_mql_cnt
    , COALESCE(b.touchpoint_crm_person_status_nurture, 0) AS touchpoint_crm_person_status_nurture_cnt
    , COALESCE(b.touchpoint_crm_person_status_qualifying, 0) AS touchpoint_crm_person_status_qualifying_cnt
    , COALESCE(b.touchpoint_crm_person_status_accepted, 0) AS touchpoint_crm_person_status_accepted_cnt
    , COALESCE(b.touchpoint_crm_person_title_cto, 0) AS touchpoint_crm_person_title_cto_cnt
    , COALESCE(b.touchpoint_crm_person_title_software_developer, 0) AS touchpoint_crm_person_title_software_developer_cnt
    , COALESCE(b.touchpoint_crm_person_title_software_engineer, 0) AS touchpoint_crm_person_title_software_engineer_cnt
    , COALESCE(b.touchpoint_crm_person_title_software_dev_team_lead, 0) AS touchpoint_crm_person_title_software_dev_team_lead_cnt
    , CASE WHEN b.dim_crm_account_id IS NOT NULL THEN 1 ELSE 0 END AS has_bizible_data_flag

-- Product Usage
    , u.unique_active_user AS unique_active_user_cnt
    , u.action_monthly_active_users_project_repo_avg AS action_monthly_active_users_project_repo_avg
    , u.merge_requests_avg AS merge_requests_avg
    , u.projects_with_repositories_enabled_avg AS projects_with_repositories_enabled_avg
    , u.ci_pipelines_avg AS ci_pipelines_avg
    , u.ci_internal_pipelines_avg AS ci_internal_pipelines_avg
    , u.ci_builds_avg AS ci_builds_avg
    , u.ci_pipeline_config_repository_avg AS ci_pipeline_config_repository_avg
    , u.user_unique_users_all_secure_scanners_avg AS user_unique_users_all_secure_scanners_avg
    , u.user_sast_jobs_avg AS user_sast_jobs_avg
    , u.user_dast_jobs_avg AS user_dast_jobs_avg
    , u.user_dependency_scanning_jobs_avg AS user_dependency_scanning_jobs_avg
    , u.user_license_management_jobs_avg AS user_license_management_jobs_avg
    , u.user_secret_detection_jobs_avg AS user_secret_detection_jobs_avg
    , u.user_container_scanning_jobs_avg AS user_container_scanning_jobs_avg
    , u.deployments_avg as deployments_avg
    , u.releases_avg as releases_avg
    , u.epics_avg as epics_avg
    , u.issues_avg as issues_avg
    , u.analytics_avg as analytics_avg
    , u.commit_comment_all_time_event AS commit_comment_all_time_event_cnt
    , u.source_code_pushes_all_time_event AS source_code_pushes_all_time_event_cnt
    , u.template_repositories_all_time_event AS template_repositories_all_time_event_cnt
    , u.ci_runners_all_time_event AS ci_runners_all_time_event_cnt
    , u.auto_devops_enabled_all_time_event AS auto_devops_enabled_all_time_event_cnt
    , u.projects_with_packages_all_time_event as projects_with_packages_all_time_event_cnt
    , u.merge_requests_all_time_event as merge_requests_all_time_event_cnt
    , u.epics_all_time_event as epics_all_time_event_cnt
    , u.issues_all_time_event as issues_all_time_event_cnt
    , u.projects_all_time_event as projects_all_time_event_cnt
    , u.gitlab_shared_runners_enabled as gitlab_shared_runners_enabled_flag
    , u.container_registry_enabled as container_registry_enabled_flag
    , CASE WHEN u.license_user_count_cnt > 0 THEN u.active_user_count_cnt / u.license_user_count_cnt  ELSE 0 END AS license_utilization_pct
    , u.active_user_count_cnt AS active_user_count_cnt
    , u.license_user_count_cnt AS license_user_count_cnt
    , u.billable_user_count_cnt AS billable_user_count_cnt
    , u.max_historical_user_count as max_historical_user_cnt
    , u.gitlab_version AS gitlab_version
    , CASE WHEN u.dim_crm_account_id IS NOT NULL THEN 1 ELSE 0 END AS has_usage_data_flag

FROM period_1 p1
LEFT JOIN period_2 p2
    ON p1.dim_crm_account_id = p2.dim_crm_account_id
LEFT JOIN opps o
    ON p1.dim_crm_account_id = o.account_id
LEFT JOIN events_salesforce es
    ON p1.dim_crm_account_id = es.account_id
LEFT JOIN tasks_salesforce ts
    ON p1.dim_crm_account_id = ts.account_id
LEFT JOIN zi_technologies zt
    ON p1.dim_crm_account_id = zt.account_id
--LEFT JOIN lifetime l
--    ON p1.dim_crm_account_id = l.dim_crm_account_id
LEFT JOIN bizible b
    ON p1.dim_crm_account_id = b.dim_crm_account_id
LEFT JOIN product_usage u
    ON p1.dim_crm_account_id = u.dim_crm_account_id

{%- endmacro -%}