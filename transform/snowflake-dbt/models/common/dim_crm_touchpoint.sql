WITH campaign_details AS (

    SELECT *
    FROM {{ ref('prep_campaign') }}

), bizible_touchpoints AS (

    SELECT *
    FROM {{ ref('sfdc_bizible_touchpoint_source') }}
    WHERE is_deleted = 'FALSE'

), bizible_attribution_touchpoints AS (

    SELECT *
    FROM {{ ref('sfdc_bizible_attribution_touchpoint_source') }}
    WHERE is_deleted = 'FALSE'

), bizible_touchpoints_with_campaign AS (

    SELECT
      bizible_touchpoints.*,
      campaign_details.dim_campaign_id,
      campaign_details.dim_parent_campaign_id
    FROM bizible_touchpoints
    LEFT JOIN campaign_details
      On bizible_touchpoints.campaign_id = campaign_details.dim_campaign_id

), bizible_attribution_touchpoints_with_campaign AS (

    SELECT
      bizible_attribution_touchpoints.*,
      campaign_details.dim_campaign_id,
      campaign_details.dim_parent_campaign_id
    FROM bizible_attribution_touchpoints
    LEFT JOIN campaign_details
      On bizible_attribution_touchpoints.campaign_id = campaign_details.dim_campaign_id

), combined_touchpoints AS (

    SELECT
      --ids
      touchpoint_id                 AS dim_crm_touchpoint_id,
      -- touchpoint info
      bizible_touchpoint_date,
      bizible_touchpoint_position,
      bizible_touchpoint_source,
      bizible_touchpoint_type,
      bizible_ad_campaign_name,
      bizible_ad_content,
      bizible_ad_group_name,
      bizible_form_url,
      bizible_form_url_raw,
      bizible_landing_page,
      bizible_landing_page_raw,
      bizible_marketing_channel,
      bizible_marketing_channel_path,
      bizible_medium,
      bizible_referrer_page,
      bizible_referrer_page_raw,
      '0'                           AS is_attribution_touchpoint,
      dim_campaign_id,
      dim_parent_campaign_id

    FROM bizible_touchpoints_with_campaign

    UNION ALL

    SELECT
      --ids
      touchpoint_id                 AS dim_crm_touchpoint_id,
      -- touchpoint info
      bizible_touchpoint_date,
      bizible_touchpoint_position,
      bizible_touchpoint_source,
      bizible_touchpoint_type,
      bizible_ad_campaign_name,
      bizible_ad_content,
      bizible_ad_group_name,
      bizible_form_url,
      bizible_form_url_raw,
      bizible_landing_page,
      bizible_landing_page_raw,
      bizible_marketing_channel,
      marketing_channel_path        AS bizible_marketing_channel_path,
      bizible_medium,
      bizible_referrer_page,
      bizible_referrer_page_raw,
      '1'                           AS is_attribution_touchpoint,
      dim_campaign_id,
      dim_parent_campaign_id

    FROM bizible_attribution_touchpoints_with_campaign

), final AS (

    SELECT
      dim_crm_touchpoint_id,
      bizible_touchpoint_date,
      bizible_touchpoint_position,
      bizible_touchpoint_source,
      bizible_touchpoint_type,
      bizible_ad_campaign_name,
      bizible_ad_content,
      bizible_ad_group_name,
      bizible_form_url,
      bizible_form_url_raw,
      bizible_landing_page,
      bizible_landing_page_raw,
      bizible_marketing_channel,
      bizible_marketing_channel_path,
      bizible_medium,
      bizible_referrer_page,
      bizible_referrer_page_raw,
      is_attribution_touchpoint,
      CASE
        WHEN dim_parent_campaign_id = '7014M000001dowZQAQ' -- based on issue https://gitlab.com/gitlab-com/marketing/marketing-strategy-performance/-/issues/246
          OR (bizible_medium = 'sponsorship'
            AND bizible_touchpoint_source IN ('issa','stackoverflow','securityweekly-appsec'))
          THEN 'Publishers/Sponsorships'
        WHEN  (bizible_touchpoint_type = 'Web Form'
          AND (bizible_landing_page LIKE '%smbnurture%'
            OR bizible_form_url LIKE '%smbnurture%'
              OR bizible_referrer_page LIKE '%smbnurture%'
                OR bizible_ad_campaign_name LIKE '%smbnurture%'
                  OR bizible_landing_page LIKE '%smbagnostic%'
                    OR bizible_form_url LIKE '%smbagnostic%'
                      OR bizible_referrer_page LIKE '%smbagnostic%'
                        OR bizible_ad_campaign_name LIKE '%smbagnostic%'))
                          OR bizible_ad_campaign_name = 'Nurture - SMB Mixed Use Case'
          THEN 'SMB Nurture'
        WHEN  (bizible_touchpoint_type = 'Web Form'
          AND (bizible_landing_page LIKE '%cicdseeingisbelieving%'
            OR bizible_form_url LIKE '%cicdseeingisbelieving%'
              OR bizible_referrer_page LIKE '%cicdseeingisbelieving%'
                OR bizible_ad_campaign_name LIKE '%cicdseeingisbelieving%'))
                  OR dim_parent_campaign_id = '7014M000001dmNAQAY'
          THEN 'CI/CD Seeing is Believing'
        WHEN  (bizible_touchpoint_type = 'Web Form'
          AND (bizible_landing_page LIKE '%simplifydevops%'
            OR bizible_form_url LIKE '%simplifydevops%'
              OR bizible_referrer_page LIKE '%simplifydevops%'
                OR bizible_ad_campaign_name LIKE '%simplifydevops%'))
                  OR dim_parent_campaign_id = '7014M000001doAGQAY'
          THEN 'Simplify DevOps'
        WHEN  (bizible_touchpoint_type = 'Web Form'
          AND (bizible_landing_page LIKE '%21q4-jp%'
            OR bizible_form_url LIKE '%21q4-jp%'
              OR bizible_referrer_page LIKE '%21q4-jp%'
                OR bizible_ad_campaign_name LIKE '%21q4-jp%'))
                  OR (dim_parent_campaign_id = '7014M000001dn8MQAQ'
                    AND bizible_ad_campaign_name ='2021_Social_Japan_LinkedIn Lead Gen')
          THEN 'Japan-Digital Readiness'
        WHEN  (bizible_touchpoint_type = 'Web Form'
          AND (bizible_landing_page LIKE '%lower-tco%'
            OR bizible_form_url LIKE '%lower-tco%'
              OR bizible_referrer_page LIKE '%lower-tco%'
                OR bizible_ad_campaign_name LIKE '%operationalefficiencies%'
                  OR bizible_ad_campaign_name LIKE '%operationalefficiences%'))
                    OR (dim_parent_campaign_id = '7014M000001dn8MQAQ'
                      AND (bizible_ad_campaign_name LIKE '%_Operational Efficiencies%'
                        OR bizible_ad_campaign_name LIKE '%operationalefficiencies%'))
          THEN 'Increase Operational Efficiencies'
        WHEN (bizible_touchpoint_type = 'Web Form'
          AND (bizible_landing_page LIKE '%reduce-cycle-time%'
            OR bizible_form_url LIKE '%reduce-cycle-time%'
              OR bizible_referrer_page LIKE '%reduce-cycle-time%'
                OR bizible_ad_campaign_name LIKE '%betterproductsfaster%'))
                  OR (dim_parent_campaign_id = '7014M000001dn8MQAQ'
                    AND (bizible_ad_campaign_name LIKE '%_Better Products Faster%'
                     OR bizible_ad_campaign_name LIKE '%betterproductsfaster%'))
          THEN 'Deliver Better Products Faster'
        WHEN (bizible_touchpoint_type = 'Web Form'
          AND (bizible_landing_page LIKE '%secure-apps%'
            OR bizible_form_url LIKE '%secure-apps%'
              OR bizible_referrer_page LIKE '%secure-apps%'
                OR bizible_ad_campaign_name LIKE '%reducesecurityrisk%'))
                  OR (dim_parent_campaign_id = '7014M000001dn8MQAQ'
                    AND (bizible_ad_campaign_name LIKE '%_Reduce Security Risk%'
                      OR bizible_ad_campaign_name LIKE '%reducesecurityrisk%'))
          THEN 'Reduce Security and Compliance Risk'
        WHEN (bizible_touchpoint_type = 'Web Form'
          AND (bizible_landing_page LIKE '%jenkins-alternative%'
            OR bizible_form_url LIKE '%jenkins-alternative%'
              OR bizible_referrer_page LIKE '%jenkins-alternative%'
                OR bizible_ad_campaign_name LIKE '%cicdcmp2%'))
                  OR (dim_parent_campaign_id = '7014M000001dn8MQAQ'
                    AND (bizible_ad_campaign_name LIKE '%_Jenkins%'
                      OR bizible_ad_campaign_name LIKE '%cicdcmp2%'))
          THEN 'Jenkins Take Out'
        WHEN (bizible_touchpoint_type = 'Web Form'
          AND (bizible_landing_page LIKE '%single-application-ci%'
            OR bizible_form_url LIKE '%single-application-ci%'
              OR bizible_referrer_page LIKE '%single-application-ci%'
                OR bizible_ad_campaign_name LIKE '%cicdcmp3%'))
                  OR (dim_parent_campaign_id = '7014M000001dn8MQAQ'
                    AND bizible_ad_campaign_name LIKE '%cicdcmp3%')
          THEN 'CI Build & Test Auto'
        WHEN (bizible_touchpoint_type = 'Web Form'
          AND (bizible_landing_page LIKE '%github-actions-alternative%'
            OR bizible_form_url LIKE '%github-actions-alternative%'
              OR bizible_referrer_page LIKE '%github-actions-alternative%'
                OR bizible_ad_campaign_name LIKE '%octocat%'))
                  OR (dim_parent_campaign_id = '7014M000001dn8MQAQ'
                    AND bizible_ad_campaign_name ILIKE '%_OctoCat%')
          THEN 'OctoCat'
        WHEN (bizible_touchpoint_type = 'Web Form'
          AND (bizible_landing_page LIKE '%integration-continue-pour-construire-et-tester-plus-rapidement%'
            OR bizible_form_url LIKE '%integration-continue-pour-construire-et-tester-plus-rapidement%'
              OR bizible_referrer_page LIKE '%integration-continue-pour-construire-et-tester-plus-rapidement%'
                OR (bizible_ad_campaign_name LIKE '%singleappci%' AND bizible_ad_content LIKE '%french%')))
                  OR (dim_parent_campaign_id = '7014M000001dn8MQAQ'
                    AND bizible_ad_campaign_name ILIKE '%Singleappci_French%')
          THEN 'CI Use Case - FR'
        WHEN (bizible_touchpoint_type = 'Web Form'
          AND (bizible_landing_page LIKE '%nutze-continuous-integration-fuer-schnelleres-bauen-und-testen%'
            OR bizible_form_url LIKE '%nutze-continuous-integration-fuer-schnelleres-bauen-und-testen%'
              OR bizible_referrer_page LIKE '%nutze-continuous-integration-fuer-schnelleres-bauen-und-testen%'
                OR (bizible_ad_campaign_name LIKE '%singleappci%' AND bizible_ad_content LIKE '%paesslergerman%')))
                  OR (dim_parent_campaign_id = '7014M000001dn8MQAQ'
                    AND bizible_ad_campaign_name ILIKE '%Singleappci_German%')
          THEN 'CI Use Case - DE'
        WHEN (bizible_touchpoint_type = 'Web Form'
          AND (bizible_landing_page LIKE '%use-continuous-integration-to-build-and-test-faster%'
            OR bizible_form_url LIKE '%use-continuous-integration-to-build-and-test-faster%'
              OR bizible_referrer_page LIKE '%use-continuous-integration-to-build-and-test-faster%'
                OR bizible_ad_campaign_name LIKE '%singleappci%'))
                  OR bizible_ad_campaign_name ='20201013_ActualTechMedia_DeepMonitoringCI'
                    OR (dim_parent_campaign_id = '7014M000001dn8MQAQ'
                      AND (bizible_ad_campaign_name LIKE '%_CI%'
                        OR bizible_ad_campaign_name ILIKE '%singleappci%'))
          THEN 'CI Use Case'
        WHEN (bizible_touchpoint_type = 'Web Form'
          AND (bizible_landing_page LIKE '%shift-your-security-scanning-left%'
            OR bizible_form_url LIKE '%shift-your-security-scanning-left%'
              OR bizible_referrer_page LIKE '%shift-your-security-scanning-left%'
                OR bizible_ad_campaign_name LIKE '%devsecopsusecase%'))
                  OR dim_parent_campaign_id = '7014M000001dnVOQAY' -- GCP Partner campaign
                    OR (dim_parent_campaign_id = '7014M000001dn8MQAQ'
                      AND (bizible_ad_campaign_name ILIKE '%_DevSecOps%'
                        OR bizible_ad_campaign_name LIKE '%devsecopsusecase%'))
          THEN 'DevSecOps Use Case'
        WHEN (bizible_touchpoint_type = 'Web Form'
          AND (bizible_landing_page LIKE '%aws-gitlab-serverless%'
            OR bizible_landing_page LIKE '%trek10-aws-cicd%'
              OR bizible_form_url LIKE '%aws-gitlab-serverless%'
                OR bizible_form_url LIKE '%trek10-aws-cicd%'
                  OR bizible_referrer_page LIKE '%aws-gitlab-serverless%'
                    OR bizible_ad_campaign_name LIKE '%awspartner%'))
                      OR (dim_parent_campaign_id = '7014M000001dn8MQAQ'
                        AND bizible_ad_campaign_name ILIKE '%_AWS%')
          THEN 'AWS'
        WHEN (bizible_touchpoint_type = 'Web Form'
          AND (bizible_landing_page LIKE '%simplify-collaboration-with-version-control%'
            OR bizible_form_url LIKE '%simplify-collaboration-with-version-control%'
              OR bizible_referrer_page LIKE '%simplify-collaboration-with-version-control%'
                OR bizible_ad_campaign_name LIKE '%vccusecase%'))
                  OR (dim_parent_campaign_id = '7014M000001dn8MQAQ'
                    AND (bizible_ad_campaign_name LIKE '%_VCC%'
                      OR bizible_ad_campaign_name LIKE '%vccusecase%'))
          THEN 'VCC Use Case'
        WHEN (bizible_touchpoint_type = 'Web Form'
          AND (bizible_landing_page LIKE '%gitops-infrastructure-automation%'
            OR bizible_form_url LIKE '%gitops-infrastructure-automation%'
              OR bizible_referrer_page LIKE '%gitops-infrastructure-automation%'
                OR bizible_ad_campaign_name LIKE '%iacgitops%'))
                  OR (dim_parent_campaign_id = '7014M000001dn8MQAQ'
                    AND (bizible_ad_campaign_name LIKE '%_GitOps%'
                      OR bizible_ad_campaign_name LIKE '%iacgitops%'))
          THEN 'GitOps Use Case'
        WHEN  (bizible_touchpoint_type = 'Web Form'
          AND (bizible_ad_campaign_name LIKE '%evergreen%'
            OR bizible_form_url_raw LIKE '%utm_campaign=evergreen%'
              OR bizible_landing_page_raw LIKE '%utm_campaign=evergreen%'
                OR bizible_referrer_page_RAW LIKE '%utm_campaign=evergreen%'))
                  OR (dim_parent_campaign_id = '7014M000001dn8MQAQ'
                    AND bizible_ad_campaign_name ILIKE '%_Evergreen%')
          Then 'Evergreen'
        WHEN (bizible_touchpoint_type = 'Web Form'
          AND (bizible_ad_campaign_name LIKE 'brand%'
            OR bizible_ad_campaign_name LIKE 'Brand%'
              OR bizible_form_url_raw LIKE '%utm_campaign=brand%'
                OR bizible_landing_page_raw LIKE '%utm_campaign=brand%'
                  OR bizible_referrer_page_RAW LIKE '%utm_campaign=brand%'))
                    OR (dim_parent_campaign_id = '7014M000001dn8MQAQ'
                      AND bizible_ad_campaign_name ILIKE '%_Brand%')
          Then 'Brand'
        Else 'None'
      END                                                                                               AS bizible_integrated_campaign_grouping,
      IFF(bizible_integrated_campaign_grouping <> 'None','Demand Gen','Other')                          AS touchpoint_segment,
      CASE
        WHEN bizible_integrated_campaign_grouping IN ('CI Build & Test Auto','CI Use Case','CI Use Case - FR','CI Use Case - DE','CI/CD Seeing is Believing','Jenkins Take Out','OctoCat')
          THEN 'CI/CD'
        WHEN bizible_integrated_campaign_grouping IN ('Deliver Better Products Faster','DevSecOps Use Case','Reduce Security and Compliance Risk','Simplify DevOps')
          THEN 'DevOps'
        WHEN bizible_integrated_campaign_grouping='GitOps Use Case' THEN 'GitOps'
        ELSE NULL
      END                                                                                               AS gtm_motion,
      CASE
        WHEN dim_crm_touchpoint_id ILIKE 'a6061000000CeS0%' -- Specific touchpoint overrides
          THEN 'Field Event'
        WHEN bizible_marketing_channel_path = 'CPC.AdWords'
          THEN 'Google AdWords'
        WHEN bizible_marketing_channel_path IN ('Email.Other', 'Email.Newsletter','Email.Outreach')
          THEN 'Email'
        WHEN bizible_marketing_channel_path IN ('Field Event','Partners.Google','Brand.Corporate Event','Conference','Speaking Session')
          OR (bizible_medium = 'Field Event (old)' AND bizible_marketing_channel_path = 'Other')
          THEN 'Field Event'
        WHEN bizible_marketing_channel_path IN ('Paid Social.Facebook','Paid Social.LinkedIn','Paid Social.Twitter','Paid Social.YouTube')
          THEN 'Paid Social'
        WHEN bizible_marketing_channel_path IN ('Social.Facebook','Social.LinkedIn','Social.Twitter','Social.YouTube')
          THEN 'Social'
        WHEN bizible_marketing_channel_path IN ('Marketing Site.Web Referral','Web Referral')
          THEN 'Web Referral'
        WHEN bizible_marketing_channel_path IN ('Marketing Site.Web Direct', 'Web Direct') -- Added to Web Direct
          OR dim_campaign_id IN (
                                 '701610000008ciRAAQ', -- Trial - GitLab.com
                                 '70161000000VwZbAAK', -- Trial - Self-Managed
                                 '70161000000VwZgAAK', -- Trial - SaaS
                                 '70161000000CnSLAA0', -- 20181218_DevOpsVirtual
                                 '701610000008cDYAAY'  -- 2018_MovingToGitLab
                                )
          THEN 'Web Direct'
        WHEN bizible_marketing_channel_path LIKE 'Organic Search.%'
          OR bizible_marketing_channel_path = 'Marketing Site.Organic'
          THEN 'Organic Search'
        WHEN bizible_marketing_channel_path IN ('Sponsorship')
          THEN 'Paid Sponsorship'
        ELSE 'Unknown'
      END                                                                                                 AS integrated_campaign_grouping

    FROM combined_touchpoints
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2021-01-21",
    updated_date="2021-03-01"
) }}
