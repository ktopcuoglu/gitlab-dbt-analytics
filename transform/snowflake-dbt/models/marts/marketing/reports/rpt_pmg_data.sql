{{config({
    "schema": "common_mart_marketing"
  })
}}

{{ simple_cte([
    ('pmg_paid_digital','pmg_paid_digital')
]) }}

, final AS (

    SELECT *,
      DATE_TRUNC('month',reporting_date)::date::date AS reporting_date_month_yr,
      reporting_date::date AS reporting_date_normalized,
      CASE
        WHEN campaign LIKE '%_apac_%' THEN 'APAC'
        WHEN campaign LIKE '%_APAC_%' THEN 'APAC'
        WHEN campaign LIKE '%_emea_%' THEN 'EMEA'
        WHEN campaign LIKE'%_EMEA_%' THEN 'EMEA'
        WHEN campaign LIKE '%_amer_%' THEN 'AMER'
        WHEN campaign LIKE '%_AMER_%'THEN 'AMER'
        WHEN campaign LIKE '%_sanfrancisco_%' THEN 'AMER'
        WHEN campaign LIKE '%_seattle_%'THEN 'AMER'
        WHEN campaign LIKE '%_lam_%' THEN 'LATAM'
        WHEN campaign LIKE '%_sam_%' THEN 'LATAM'
      ELSE 'Global' END AS region_normalized,
      CASE 
        WHEN medium = 'cpc' THEN 'Paid Search'
        WHEN medium = 'display' THEN 'Display'
        WHEN medium = 'paidsocial' THEN 'Paid Social'
      ELSE 'Other' END AS mapped_channel,
      CASE
        WHEN source = 'google' THEN 'Google'
        WHEN source = 'bing_yahoo' THEN 'Bing'
        WHEN source = 'facebook' THEN 'Facebook'
        WHEN source = 'linkedin' THEN 'LinkedIn'
        WHEN source = 'twitter' THEN 'Twitter'
      ELSE 'Other' END AS mapped_source,
      CONCAT((mapped_channel),'.',(mapped_source)) AS mapped_channel_source,
      CASE 
        WHEN mapped_channel_source = 'Other.Other' THEN 'Other'
        WHEN mapped_channel_source = 'Paid Search.Google' THEN 'Paid Search.AdWords'
      ELSE mapped_channel_source END AS mapped_channel_source_normalized,
      CASE 
        WHEN CONTENT_TYPE = 'Free Trial' THEN 'Free Trial'
        WHEN team='digital' and medium='sponsorship'  THEN 'Publishers/Sponsorships' -- team=digital  captures the paid ads for digital and not including field 
        WHEN campaign_code LIKE '%operationalefficiencies%' THEN 'Increase Operational Efficiencies'
        WHEN campaign_code LIKE '%operationalefficiences%' THEN 'Increase Operational Efficiencies'
        WHEN campaign_code LIKE '%betterproductsfaster%' THEN 'Deliver Better Products Faster'
        WHEN campaign_code LIKE '%reducesecurityrisk%' THEN 'Reduce Security and Compliance Risk'
        WHEN campaign_code LIKE '%cicdcmp2%' THEN 'Jenkins Take Out'
        WHEN campaign_code LIKE '%cicdcmp3%' THEN 'CI Build & Test Auto'
        WHEN campaign_code LIKE '%octocat%' THEN 'OctoCat'
        WHEN campaign_code LIKE '%21q4-jp%' THEN 'Japan-Digital Readiness'
        WHEN (campaign_code LIKE '%singleappci%' AND campaign LIKE '%france%')THEN 'CI Use Case - FR'
        WHEN (campaign_code LIKE '%singleappci%' AND campaign LIKE '%germany%')THEN 'CI Use Case - DE'
        WHEN campaign_code LIKE '%singleappci%' THEN 'CI Use Case'
        WHEN campaign_code LIKE '%devsecopsusecase%' THEN 'DevSecOps Use Case'
        WHEN campaign_code LIKE '%awspartner%' THEN 'AWS'
        WHEN campaign_code LIKE '%vccusecase%' then 'VCC Use Case'
        WHEN campaign_code LIKE '%iacgitops%' THEN 'GitOps Use Case'
        WHEN campaign_code LIKE '%evergreen%' THEN 'Evergreen'
        WHEN campaign_code LIKE 'brand%' THEN 'Brand'
        WHEN campaign_code LIKE 'Brand%' THEN 'Brand'
        WHEN campaign_code LIKE '%simplifydevops%' THEN 'Simplify DevOps'
        WHEN campaign_code LIKE '%premtoultimatesp%' THEN 'Premium to Ultimate'
        WHEN campaign_code LIKE '%devopsgtm%' THEN 'DevOps GTM'
        WHEN campaign_code LIKE '%gitlab14%' THEN 'GitLab 14 webcast'
        WHEN campaign_code LIKE '%devopsgtm%' AND content LIKE '%partnercredit%' THEN 'Cloud Partner  Campaign' 
        WHEN campaign_code LIKE '%devopsgtm%' AND content LIKE '%introtomlopsdemo%' THEN 'Technical  Demo Series'
        WHEN campaign_code LIKE '%psdigitaltransformation%' OR campaign_code LIKE '%psglobal%' THEN 'PubSec Nurture'
      ELSE 'None' END AS integrated_campaign_grouping,
      CASE 
        WHEN budget LIKE '%x-ent%' THEN 'Large'
        WHEN budget LIKE '%x-mm%' THEN 'Mid-Market'
        WHEN budget LIKE '%x-smb%' THEN 'SMB'
        WHEN budget LIKE '%x-pr%' THEN 'Prospecting'
        WHEN budget LIKE '%x-rtg%' THEN 'Retargeting'
      ELSE NULL
      END AS utm_segment,
      IFF(integrated_campaign_grouping <> 'None','Demand Gen','Other') AS touchpoint_segment,
      CASE
        WHEN integrated_campaign_grouping IN ('CI Build & Test Auto','CI Use Case','CI Use Case - FR','CI Use Case - DE','CI/CD Seeing is Believing','Jenkins Take Out','OctoCat','Premium to Ultimate') THEN 'CI/CD'
        WHEN integrated_campaign_grouping IN ('Deliver Better Products Faster','DevSecOps Use Case','Reduce Security and Compliance Risk','Simplify DevOps', 'DevOps GTM', 'Cloud Partner Campaign', 'GitLab 14 webcast', 'Technical Demo Series') THEN 'DevOps'
        WHEN integrated_campaign_grouping='GitOps Use Case' THEN 'GitOps'
      ELSE NULL 
      END AS gtm_motion
    FROM pmg_paid_digital

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2022-01-25",
    updated_date="2022-02-02"
) }}
