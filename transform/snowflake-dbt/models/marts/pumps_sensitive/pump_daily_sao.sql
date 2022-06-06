{{ config(
    tags=["mnpi_exception", "pmg"]
) }}

WITH base AS (--NOTE: ONLY add columns to the END of the final query per PMG
    WITH tp AS(
        WITH bplc AS (
            WITH ca AS (

                SELECT
                    contact_id                            AS contact_id,
                    a.account_id                          AS account_id,
                    a.ultimate_parent_account_id          AS ultimate_parent_account_id,
                    a.ultimate_parent_account_name        AS ultimate_parent_account_name,
                    a.account_name                        AS account_name,
                    a.gtm_strategy                        AS gtm_strategy,
                    a.account_demographics_region         AS region,
                    a.sales_segment                       AS sales_segmentation,
                    inquiry_datetime                      AS inquiry_datetime,
                    marketo_qualified_lead_date           AS marketo_qualified_lead_date,
                    mql_datetime_inferred                 AS mql_datetime_inferred,
                    accepted_datetime                     AS accepted_datetime,
                    contact_status                        AS contact_status,
                    contact_title                         AS contact_title,
                    mailing_country                       AS mailing_country,
                    last_utm_campaign                     AS last_utm_campaign,
                    last_utm_content                      AS last_utm_content,
                    lead_source                           AS lead_source
            FROM {{ REF('sfdc_contact_xf') }} C
            LEFT JOIN {{ REF('sfdc_accounts_xf') }}  a ON C.account_id = a.account_id
)

    SELECT
      bp.person_id                                                AS person_id,
      bp.bizible_lead_id                                          AS bizible_lead_id,
      bp.bizible_contact_id                                       AS bizible_contact_id,
      l.inquiry_datetime                                          AS lead_inquiry_datetime,
      ca.inquiry_datetime                                         AS contact_inquiry_datetime,
      l.marketo_qualified_lead_date                               AS lead_mql_datetime,
      l.mql_datetime_inferred                                     AS lead_mql_datetime_inferred,
      ca.marketo_qualified_lead_date                              AS contact_mql_datetime,
      ca.mql_datetime_inferred                                    AS contact_mql_datetime_inferred,
      l.accepted_datetime                                         AS lead_accepted_datetime,
      ca.accepted_datetime                                        AS contact_accepted_datetime,
      l.lead_status                                               AS lead_status,
      ca.contact_status                                           AS contact_status,
      l.title                                                     AS lead_title,
      ca.contact_title                                            AS contact_title,
      l.account_demographics_region                               AS region,
      ca.region                                                   AS account_region,
      l.sales_segmentation                                        AS sales_segmentation,
      ca.sales_segmentation                                       AS account_sales_segmentation,
      l.country                                                   AS country,
      ca.mailing_country                                          AS mailing_country,
      l.company                                                   AS company,
      ca.account_name                                             AS account_name,
      ca.gtm_strategy                                             AS gtm_strategy,
      ca.account_id                                               AS account_id,
      ca.ultimate_parent_account_id                               AS ultimate_parent_account_id,
      ca.ultimate_parent_account_name                             AS ultimate_parent_account_name,
      l.last_utm_campaign                                         AS last_utm_campaign,
      l.last_utm_content                                          AS last_utm_content,
      ca.last_utm_campaign                                        AS last_utm_campaign,
      ca.last_utm_content                                         AS last_utm_content,
      l.lead_source                                               AS lead_source,
      ca.lead_source                                              AS lead_source,
      CASE
      WHEN l.lead_source IS NOT NULL THEN l.lead_source
      ELSE ca.lead_source END AS person_lead_source,
      CASE
      WHEN bp.bizible_contact_id  IS NOT NULL THEN bp.bizible_contact_id
      ELSE bp.bizible_lead_id END AS sfdc_person_id,
      CASE
      WHEN ca.region IS NOT NULL THEN ca.region
      ELSE l.account_demographics_region
      END AS person_region,
      CASE
      WHEN ca.sales_segmentation IS NOT NULL THEN ca.sales_segmentation
      ELSE l.sales_segmentation
      END AS person_sales_segmentation,
      CASE
      WHEN ca.mailing_country IS NOT NULL THEN ca.mailing_country
      ELSE l.country
      END AS person_country,
      CASE
      WHEN ca.contact_status IS NOT NULL THEN ca.contact_status
      ELSE l.lead_status
      END AS person_status,
      CASE
      WHEN ca.contact_title IS NOT NULL THEN ca.contact_title
      ELSE l.title
      END AS person_title,
      CASE
      WHEN ca.account_name IS NOT NULL THEN ca.account_name
      ELSE l.company
      END AS person_company,
      CASE
      WHEN ca.last_utm_campaign IS NOT NULL THEN ca.last_utm_campaign
      ELSE l.last_utm_campaign
      END AS person_last_utm_campaign,
      CASE
      WHEN ca.last_utm_content IS NOT NULL THEN ca.last_utm_content
      ELSE l.last_utm_content
      END AS person_last_utm_content,
      CASE
      WHEN ca.inquiry_datetime IS NOT NULL THEN ca.inquiry_datetime::DATE
      ELSE l.inquiry_datetime::DATE
      END AS person_inquiry_datetime,
      CASE
      WHEN ca.marketo_qualified_lead_date IS NOT NULL THEN ca.marketo_qualified_lead_date::DATE
      ELSE l.marketo_qualified_lead_date::DATE
      END AS person_mql_datetime,
      CASE
      WHEN ca.mql_datetime_inferred IS NOT NULL THEN ca.mql_datetime_inferred::DATE
      ELSE l.mql_datetime_inferred::DATE
      END AS person_mql_datetime_inferred,
      CASE
      WHEN ca.accepted_datetime IS NOT NULL THEN ca.accepted_datetime::DATE
      ELSE l.accepted_datetime::DATE
      END AS person_accepted_datetime

    FROM {{ ref('sfdc_bizible_person') }} bp
    LEFT JOIN {{ ref('sfdc_lead_xf') }} l ON bp.bizible_lead_id = l.lead_id
    LEFT JOIN ca ON bp.bizible_contact_id = ca.contact_id
    WHERE bp.is_deleted = FALSE

), campaign AS (

    SELECT *
    FROM {{ ref('sfdc_campaign') }}

)

SELECT
date_trunc('month',tp.bizible_touchpoint_date)::DATE::DATE  AS bizible_touchpoint_date_month_yr,
tp.bizible_touchpoint_date::DATE                            AS bizible_touchpoint_date_normalized,
tp.bizible_touchpoint_date,
tp.touchpoint_id,
tp.bizible_touchpoint_type,
camp.type AS campaign_type,
tp.bizible_touchpoint_source,
tp.bizible_medium,
1 AS Touchpoint_count,
tp.bizible_person_id,
bplc.sfdc_person_id,
tp.bizible_count_lead_creation_touch,
CASE
WHEN camp.campaign_parent_id = '7014M000001dn8MQAQ'
   THEN 'Paid Social.LinkedIn Lead Gen'
WHEN bizible_ad_campaign_name = '20201013_ActualTechMedia_DeepMonitoringCI'
   THEN 'Sponsorship'
ELSE tp.bizible_marketing_channel_path
END AS bizible_marketing_channel_path,
tp.bizible_landing_page,
tp.bizible_form_url,
tp.bizible_referrer_page,
tp.bizible_ad_campaign_name,
tp.bizible_ad_content,
tp.bizible_form_url_raw,
tp.bizible_landing_page_raw,
tp.bizible_referrer_page_raw,
bplc.person_inquiry_datetime:: DATE                     AS person_inquiry_datetime,
bplc.person_mql_datetime:: DATE                         AS person_mql_datetime,
bplc.person_accepted_datetime:: DATE                    AS person_accepted_datetime,
bplc.person_status,
bplc.person_lead_source ,
bplc.person_last_utm_campaign,
bplc.person_last_utm_content,
CASE
  WHEN bplc.person_region = 'NORAM' THEN 'AMER'
  ELSE bplc.person_region
  END AS person_region,
IFF(bplc.person_sales_segmentation IS NULL,'Unknown',bplc.person_sales_segmentation) AS sfdc_sales_segmentation,
bplc.person_company,
bplc.account_id,
bplc.account_name,
bplc.ultimate_parent_account_id,
bplc.ultimate_parent_account_name,
bplc.gtm_strategy,
UPPER(bplc.person_title)        AS person_title,
UPPER(bplc.country)             AS person_country,
CASE
 WHEN bplc.person_mql_datetime >= bizible_touchpoint_date_normalized THEN '1'
 ELSE '0'
 END AS count_mql,
CASE
 WHEN count_mql=1 THEN bplc.sfdc_person_id
 ELSE NULL
 END AS mql_person,
CASE
 WHEN bplc.person_mql_datetime >= bizible_touchpoint_date_normalized THEN tp.bizible_count_lead_creation_touch
 ELSE '0'
 END AS count_net_new_mql,
CASE
 WHEN bplc.person_accepted_datetime >= bizible_touchpoint_date_normalized THEN '1'
 ELSE '0'
 END AS count_accepted,
CASE
 WHEN bplc.person_accepted_datetime >= bizible_touchpoint_date_normalized THEN tp.bizible_count_lead_creation_touch
 ELSE '0'
 END AS count_net_new_accepted,
CASE
   WHEN camp.campaign_parent_id = '7014M000001dowZQAQ' -- based on issue https://gitlab.com/gitlab-com/marketing/marketing-strategy-performance/-/issues/246
    OR (bizible_medium = 'sponsorship'
      AND bizible_touchpoint_source IN ('issa','stackoverflow','securityweekly-appsec','unix&linux','stackexchange'))
   THEN 'Publishers/Sponsorships'
   WHEN  (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%smbnurture%'
    OR bizible_form_url LIKE '%smbnurture%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%smbnurture%'
    OR bizible_ad_campaign_name LIKE '%smbnurture%'
    OR bizible_landing_page LIKE '%smbagnostic%'
    OR bizible_form_url LIKE '%smbagnostic%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%smbagnostic%'
    OR bizible_ad_campaign_name LIKE '%smbagnostic%'))
    OR bizible_ad_campaign_name = 'Nurture - SMB Mixed Use Case'
    THEN 'SMB Nurture'
  WHEN  (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%cicdseeingisbelieving%'
    OR bizible_form_url LIKE '%cicdseeingisbelieving%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%cicdseeingisbelieving%'
    OR bizible_ad_campaign_name LIKE '%cicdseeingisbelieving%'))
    OR bizible_ad_campaign_name = '20201215_HowCiDifferent' --added 2022-04-06 Agnes O Demand Gen issue 2330
    THEN 'CI/CD Seeing is Believing'
 WHEN  (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%simplifydevops%'
    OR bizible_form_url LIKE '%simplifydevops%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%simplifydevops%'
    OR bizible_ad_campaign_name LIKE '%simplifydevops%'))
    OR camp.campaign_parent_id = '7014M000001doAGQAY'
    OR camp.campaign_id LIKE '7014M000001dn6z%'
    THEN 'Simplify DevOps'
  WHEN  (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%21q4-jp%'
    OR bizible_form_url LIKE '%21q4-jp%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%21q4-jp%'
    OR bizible_ad_campaign_name LIKE '%21q4-jp%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ='2021_Social_Japan_LinkedIn Lead Gen')
    THEN 'Japan-Digital Readiness'
  WHEN  (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%lower-tco%'
    OR bizible_form_url LIKE '%lower-tco%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%lower-tco%'
    OR bizible_ad_campaign_name LIKE '%operationalefficiencies%'
    OR bizible_ad_campaign_name LIKE '%operationalefficiences%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name LIKE '%_Operational Efficiencies%'
        OR bizible_ad_campaign_name LIKE '%operationalefficiencies%'))
    THEN 'Increase Operational Efficiencies'
  WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%reduce-cycle-time%'
    OR bizible_form_url LIKE '%reduce-cycle-time%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%reduce-cycle-time%'
    OR bizible_ad_campaign_name LIKE '%betterproductsfaster%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name LIKE '%_Better Products Faster%'
        OR bizible_ad_campaign_name LIKE '%betterproductsfaster%'))
    THEN 'Deliver Better Products Faster'
  WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%secure-apps%'
    OR bizible_form_url LIKE '%secure-apps%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%secure-apps%'
    OR bizible_ad_campaign_name LIKE '%reducesecurityrisk%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name LIKE '%_Reduce Security Risk%'
        OR bizible_ad_campaign_name LIKE '%reducesecurityrisk%'))
    THEN 'Reduce Security and Compliance Risk'
  WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%jenkins-alternative%'
    OR bizible_form_url LIKE '%jenkins-alternative%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%jenkins-alternative%'
    OR bizible_ad_campaign_name LIKE '%cicdcmp2%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name LIKE '%_Jenkins%'
        OR bizible_ad_campaign_name LIKE '%cicdcmp2%'))
    THEN 'Jenkins Take Out'
  WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%single-application-ci%'
    OR bizible_form_url LIKE '%single-application-ci%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%single-application-ci%'
    OR bizible_ad_campaign_name LIKE '%cicdcmp3%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name LIKE '%cicdcmp3%')
    THEN 'CI Build & Test Auto'
 WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%github-actions-alternative%'
    OR bizible_form_url LIKE '%github-actions-alternative%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%github-actions-alternative%'
    OR bizible_ad_campaign_name LIKE '%octocat%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ILIKE '%_OctoCat%')
    OR  bizible_ad_campaign_name = '20200122_MakingCaseCICD'
    THEN 'OctoCat'
 WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%integration-continue-pour-construire-et-tester-plus-rapidement%'
    OR bizible_form_url LIKE '%integration-continue-pour-construire-et-tester-plus-rapidement%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%integration-continue-pour-construire-et-tester-plus-rapidement%'
    OR (bizible_ad_campaign_name LIKE '%singleappci%' AND BIZIBLE_AD_CONTENT LIKE '%french%')))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ILIKE '%Singleappci_French%')
    THEN 'CI Use Case - FR'
 WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%nutze-continuous-integration-fuer-schnelleres-bauen-und-testen%'
    OR bizible_form_url LIKE '%nutze-continuous-integration-fuer-schnelleres-bauen-und-testen%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%nutze-continuous-integration-fuer-schnelleres-bauen-und-testen%'
    OR (bizible_ad_campaign_name LIKE '%singleappci%' AND BIZIBLE_AD_CONTENT LIKE '%paesslergerman%')))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ILIKE '%Singleappci_German%')
    THEN 'CI Use Case - DE'
 WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%use-continuous-integration-to-build-and-test-faster%'
    OR bizible_form_url LIKE '%use-continuous-integration-to-build-and-test-faster%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%use-continuous-integration-to-build-and-test-faster%'
    OR bizible_ad_campaign_name LIKE '%singleappci%'))
    OR bizible_ad_campaign_name ='20201013_ActualTechMedia_DeepMonitoringCI'
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name LIKE '%_CI%'
        OR bizible_ad_campaign_name ILIKE '%singleappci%'))
    OR (camp.campaign_parent_id = '7014M000001vm9KQAQ' AND bizible_ad_campaign_name LIKE '%_CI%') --- Added by AO demand gen issue 2262
    THEN 'CI Use Case'
 WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%shift-your-security-scanning-left%'
    OR bizible_form_url LIKE '%shift-your-security-scanning-left%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%shift-your-security-scanning-left%'
    OR bizible_ad_campaign_name LIKE '%devsecopsusecase%'))
    OR camp.campaign_parent_id = '7014M000001dnVOQAY' -- GCP Partner campaign
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name ILIKE '%_DevSecOps%'
        OR bizible_ad_campaign_name LIKE '%devsecopsusecase%'))
    OR (camp.campaign_parent_id = '7014M000001vm9KQAQ' AND (
      bizible_ad_campaign_name ILIKE '%DevSecOps%'
    OR bizible_ad_campaign_name LIKE '%Fuzzing%'))-- Added by AO demand gen issue 2262
    THEN 'DevSecOps Use Case'
 WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%aws-gitlab-serverless%'
    OR bizible_landing_page LIKE '%trek10-aws-cicd%'
    OR bizible_form_url LIKE '%aws-gitlab-serverless%'
    OR bizible_form_url LIKE '%trek10-aws-cicd%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%aws-gitlab-serverless%'
    OR bizible_ad_campaign_name LIKE '%awspartner%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ILIKE '%_AWS%')
    THEN 'AWS'
 WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%simplify-collaboration-with-version-control%'
    OR bizible_form_url LIKE '%simplify-collaboration-with-version-control%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%simplify-collaboration-with-version-control%'
    OR bizible_ad_campaign_name LIKE '%vccusecase%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name LIKE '%_VCC%'
        OR bizible_ad_campaign_name LIKE '%vccusecase%'))
    THEN 'VCC Use Case'
 WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%gitops-infrastructure-automation%'
    OR bizible_form_url LIKE '%gitops-infrastructure-automation%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%gitops-infrastructure-automation%'
    OR bizible_ad_campaign_name LIKE '%iacgitops%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name LIKE '%_GitOps%'
        OR bizible_ad_campaign_name LIKE '%iacgitops%'))
    OR (camp.campaign_parent_id = '7014M000001vm9KQAQ' AND bizible_ad_campaign_name ILIKE '%GitOps_%') --- Added by AO demand gen issue 2262
    THEN 'GitOps Use Case'
 WHEN  (bizible_touchpoint_type = 'Web Form'
    AND (bizible_ad_campaign_name LIKE '%evergreen%'
    OR BIZIBLE_FORM_URL_RAW LIKE '%utm_campaign=evergreen%'
    OR BIZIBLE_LANDING_PAGE_RAW LIKE '%utm_campaign=evergreen%'
    OR BIZIBLE_REFERRER_PAGE_RAW LIKE '%utm_campaign=evergreen%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ILIKE '%_Evergreen%')
   THEN 'Evergreen'
 WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_ad_campaign_name LIKE 'brand%'
   OR bizible_ad_campaign_name LIKE 'Brand%'
    OR BIZIBLE_FORM_URL_RAW LIKE '%utm_campaign=brand%'
    OR BIZIBLE_LANDING_PAGE_RAW LIKE '%utm_campaign=brand%'
    OR BIZIBLE_REFERRER_PAGE_RAW LIKE '%utm_campaign=brand%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ILIKE '%_Brand%')
   THEN 'Brand'
 WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-04-27 MSandP: 316
    AND (bizible_ad_campaign_name LIKE 'simplifydevops%'
   OR bizible_ad_campaign_name LIKE 'Simplifydevops%'
    OR BIZIBLE_FORM_URL_RAW LIKE '%utm_campaign=simplifydevops%'
    OR BIZIBLE_LANDING_PAGE_RAW LIKE '%utm_campaign=simplifydevops%'
    OR BIZIBLE_REFERRER_PAGE_RAW LIKE '%utm_campaign=simplifydevops%'))
    OR (camp.campaign_parent_id = '7014M000001dn6zQAA')
   THEN 'Simplify DevOps'
WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-06-04 MSandP: 332
    AND (bizible_landing_page LIKE '%contact-us-ultimate%'
    OR bizible_form_url LIKE '%contact-us-ultimate%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%contact-us-ultimate%'
    OR bizible_ad_campaign_name LIKE '%premtoultimatesp%'))
    THEN 'Premium to Ultimate'
WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-06-04 MSandP: 346
    AND ( BIZIBLE_FORM_URL_RAW LIKE '%webcast-gitops-multicloudapp%'
    OR BIZIBLE_LANDING_PAGE_RAW LIKE '%webcast-gitops-multicloudapp%'
    OR BIZIBLE_REFERRER_PAGE_RAW LIKE '%webcast-gitops-multicloudapp%'))
    OR (camp.campaign_parent_id LIKE '%7014M000001dpmf%')
   THEN 'GitOps GTM webcast'
WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-06-04 MSandP: 346
    AND ( BIZIBLE_FORM_URL_RAW LIKE '%devopsgtm%'
    OR BIZIBLE_LANDING_PAGE_RAW LIKE '%devopsgtm%'
    OR BIZIBLE_REFERRER_PAGE_RAW LIKE '%devopsgtm%'))
    OR camp.campaign_parent_id LIKE '%7014M000001dpT9%'
      -- OR camp.campaign_parent_id LIKE '%7014M000001dn8M%')
    OR camp.campaign_id LIKE '%7014M000001vbtw%'
    OR (camp.campaign_parent_id = '7014M000001vm9KQAQ'
    AND (bizible_ad_campaign_name ILIKE '%MLOps%'
    OR bizible_ad_campaign_name ILIKE '%Dora%'
    OR bizible_ad_campaign_name ILIKE '%DevOps%'))-- Added by AO demand gen issue 2262
   THEN 'DevOps GTM'
WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-06-04 MSandP: 346
    AND (( bizible_form_url_raw LIKE '%utm_campaign=devopsgtm%' AND bizible_form_url_raw LIKE '%utm_content=partnercredit%'
    OR bizible_landing_page_raw LIKE '%utm_campaign=devopsgtm%' AND bizible_landing_page_raw LIKE '%utm_content=partnercredit%'
    OR bizible_referrer_page_raw LIKE '%utm_campaign=devopsgtm%' AND bizible_referrer_page_raw LIKE '%utm_content=partnercredit%')
         OR(
      bizible_form_url_raw LIKE '%cloud-credits-promo%'
      OR bizible_landing_page_raw LIKE '%cloud-credits-promo%'
      OR bizible_referrer_page_raw LIKE '%cloud-credits-promo%'
      )))
    OR camp.campaign_parent_id LIKE '%7014M000001vcDr%'
      -- OR camp.campaign_parent_id LIKE '%7014M000001dn8M%')
    OR camp.campaign_id LIKE '%7014M000001vcDr%'
   THEN 'Cloud Partner Campaign'
WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-06-04 MSandP: 346
    AND (( bizible_form_url_raw LIKE '%utm_campaign=gitlab14%'
    OR bizible_landing_page_raw LIKE '%utm_campaign=gitlab14%'
    OR bizible_referrer_page_raw LIKE '%utm_campaign=gitlab14%')
       OR(
      bizible_form_url_raw LIKE '%the-shift-to-modern-devops%'
      OR bizible_landing_page_raw LIKE '%the-shift-to-modern-devops%'
      OR bizible_referrer_page_raw LIKE '%the-shift-to-modern-devops%'
      )))
THEN 'GitLab 14 webcast'
WHEN
  camp.campaign_id LIKE '%7014M000001drcQ%'
   THEN '20210512_ISSAWebcast'
WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-0830 MSandP: 325
    AND (( bizible_form_url_raw LIKE '%psdigitaltransformation%'
    OR bizible_landing_page_raw LIKE '%psdigitaltransformation%'
    OR bizible_referrer_page_raw LIKE '%psdigitaltransformation%')
       OR(
      bizible_form_url_raw LIKE '%psglobal%'
      OR bizible_landing_page_raw LIKE '%psglobal%'
      OR bizible_referrer_page_raw LIKE '%psglobal%'
      )))
THEN 'PubSec Nurture'
WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-11-22 MSandP: 585
    AND ( bizible_form_url_raw LIKE '%whygitlabdevopsplatform-apac%'
    OR bizible_landing_page_raw LIKE '%whygitlabdevopsplatform-apac%'
    OR bizible_referrer_page_raw LIKE '%whygitlabdevopsplatform-apac%'))
    OR bizible_ad_campaign_name = '20211208_GitHubCompetitive_APAC'
THEN 'FY22 GitHub Competitive Campaign - APAC' --added 2022-04-06 Agnes O Demand Gen issue 2330
WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-11-22 MSandP: 585
    AND (( bizible_form_url_raw LIKE '%whygitlabdevopsplatform%'
    OR bizible_landing_page_raw LIKE '%whygitlabdevopsplatform%'
    OR bizible_referrer_page_raw LIKE '%whygitlabdevopsplatform%')
       OR(
      bizible_form_url_raw LIKE '%githubcompete%'
      OR bizible_landing_page_raw LIKE '%githubcompete%'
      OR bizible_referrer_page_raw LIKE '%githubcompete%'
      )))
   OR bizible_ad_campaign_name = '20211202_GitHubCompetitive'--added 2022-04-06 Agnes O Demand Gen issue 2330
THEN 'FY22 GitHub Competitive Campaign'
WHEN (bizible_touchpoint_type = 'Web Form' -- MSandP 657
     AND (BIZIBLE_FORM_URL_RAW LIKE '%utm_campaign=cdusecase%'
    OR BIZIBLE_LANDING_PAGE_RAW LIKE '%utm_campaign=cdusecase%'
    OR BIZIBLE_REFERRER_PAGE_RAW LIKE '%utm_campaign=cdusecase%'))
    OR (camp.campaign_parent_id = '7014M000001vm9KQAQ' AND bizible_ad_campaign_name LIKE '%CD_%') --- Added by AO demand gen issue 2262
   THEN 'CD Use Case'
  ELSE 'None'
  END AS bizible_integrated_campaign_grouping,
IFF(bizible_integrated_campaign_grouping <> 'None','Demand Gen','Other') AS touchpoint_segment,
CASE
    WHEN bizible_integrated_campaign_grouping IN ('CI Build & Test Auto','CI Use Case','CI Use Case - FR','CI Use Case - DE','CI/CD Seeing is Believing','Jenkins Take Out','OctoCat','Premium to Ultimate','20210512_ISSAWebcast', 'CD Use Case') THEN 'CI/CD'
WHEN bizible_integrated_campaign_grouping IN ('Deliver Better Products Faster','DevSecOps Use Case','Reduce Security and Compliance Risk','Simplify DevOps', 'DevOps GTM', 'Cloud Partner Campaign', 'GitLab 14 webcast') THEN 'DevOps'
WHEN bizible_integrated_campaign_grouping IN ('GitOps Use Case','GitOps GTM webcast')  THEN 'GitOps'
ELSE NULL END AS gtm_motion
FROM {{ ref('sfdc_bizible_touchpoint') }} tp
LEFT JOIN bplc ON bplc.person_id = tp.bizible_person_id
LEFT JOIN campaign camp ON tp.campaign_id=camp.campaign_id
WHERE tp.is_deleted = FALSE

), ol AS (
    WITH biz_base AS (
        WITH oa AS(
            SELECT
              o.opportunity_id                AS opportunity_id,
              o.created_date                  AS opp_created_date,
              o.sales_accepted_date           AS sales_accepted_date,
              o.close_date                    AS close_date,
              o.deal_path                     AS deal_path,
              o.stage_name                    AS stage_name,
              o.order_type_stamped            AS order_type_stamped,
              o.is_won                        AS is_won,
              o.incremental_acv               AS iacv,
              a.sales_segment                 AS sales_segment,
              a.account_demographics_region   AS account_region,
              a.account_id                    AS account_id,
              a.account_name                  AS account_name,
              a.gtm_strategy                  AS gtm_strategy
FROM {{ ref('sfdc_opportunity_xf') }}  o
LEFT JOIN {{ ref('sfdc_accounts_xf') }} a
ON o.account_id = a.account_id)

SELECT
  bat.*,
  opp_created_date                                        AS opp_created_date,
  sales_accepted_date                                     AS sales_accepted_date,
  close_date                                              AS close_date,
  deal_path                                               AS deal_path,
  stage_name                                              AS stage_name,
  is_won                                                  AS is_won,
  iacv                                                    AS iacv,
  IFF(sales_segment IS NULL,'Unknown',sales_segment)      AS sales_segment,
  account_region                                          AS account_region,
  account_id                                              AS account_id,
  account_name                                            AS account_name,
  gtm_strategy                                            AS gtm_strategy,
  order_type_stamped                                      AS order_type_stamped
FROM  {{ ref('sfdc_bizible_attribution_touchpoint_xf') }}  bat
LEFT JOIN oa opp
ON bat.opportunity_id = opp.opportunity_id
WHERE stage_name NOT LIKE '%Duplicate%'
AND LOWER(is_deleted) LIKE 'false'

), contacts AS (

SELECT
    contact_id          AS contact_id,
    mailing_country     AS mailing_country,
    last_utm_campaign   AS person_last_utm_campaign,
    last_utm_content    AS person_last_utm_content
FROM {{ ref('sfdc_contact_xf') }}

), campaign AS (

SELECT *
FROM {{ ref('sfdc_campaign') }}

), linear_base AS (

SELECT
    opportunity_id,
    iacv,
    COUNT(DISTINCT biz_base.touchpoint_id)  AS count_touches,
    iacv/count_touches                      AS weighted_linear_iacv
FROM biz_base
GROUP BY 1,2

), campaigns_per_opp AS (

SELECT
    opportunity_id,
    COUNT(DISTINCT biz_base.campaign_id) AS campaigns_per_opp
FROM biz_base
GROUP BY 1

)

SELECT
    biz_base.opportunity_id AS opportunity_id,
    touchpoint_id,
    biz_base.campaign_id,
    biz_base.bizible_contact,
    contacts.mailing_country AS country ,
    biz_base.iacv,
    biz_base.account_id,
    biz_base.account_name,
    biz_base.gtm_strategy,
    (biz_base.iacv / campaigns_per_opp.campaigns_per_opp) AS iacv_per_campaign ,
    count_touches,
    bizible_touchpoint_date,
    bizible_touchpoint_position,
    bizible_touchpoint_source,
    bizible_touchpoint_type ,
    bizible_ad_campaign_name,
    BIZIBLE_AD_CONTENT,
    BIZIBLE_FORM_URL_RAW,
    BIZIBLE_LANDING_PAGE_RAW,
    BIZIBLE_REFERRER_PAGE_RAW ,
    bizible_form_url,
    bizible_landing_page,
    bizible_marketing_channel,
    CASE
    WHEN camp.campaign_parent_id = '7014M000001dn8MQAQ'
      THEN 'Paid Social.LinkedIn Lead Gen'
    WHEN bizible_ad_campaign_name = '20201013_ActualTechMedia_DeepMonitoringCI'
      THEN 'Sponsorship'
    ELSE bizible_marketing_channel_path
    END AS marketing_channel_path,
    pipe_name,
    bizible_medium,
    bizible_referrer_page,
    biz_base.lead_source AS lead_source,
    opp_created_date::DATE AS opp_created_date,
    sales_accepted_date::DATE AS sales_accepted_date,
    close_date::DATE AS close_date,
    sales_qualified_month,
    biz_base.sales_type,
    biz_base.stage_name,
    biz_base.is_won,
    biz_base.deal_path,
    biz_base.order_type_stamped,
    IFF(biz_base.sales_segment IS NULL,'Unknown',biz_base.sales_segment) AS sales_segment,
    biz_base.account_region,
    date_trunc('month',bizible_touchpoint_date)::DATE::DATE AS bizible_touchpoint_date_month_yr  ,
    bizible_touchpoint_date::DATE AS bizible_touchpoint_date_normalized,
    camp.type AS campaign_type,
    contacts.person_last_utm_campaign,
    contacts.person_last_utm_content,
    CASE
    WHEN camp.campaign_parent_id = '7014M000001dowZQAQ'
    OR (bizible_medium = 'sponsorship'
      AND bizible_touchpoint_source IN ('issa','stackoverflow','securityweekly-appsec','unix&linux','stackexchange'))
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
    OR bizible_ad_campaign_name = '20201215_HowCiDifferent' --added 2022-04-06 Agnes O Demand Gen issue 2330
    THEN 'CI/CD Seeing is Believing'
    WHEN  (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%simplifydevops%'
    OR bizible_form_url LIKE '%simplifydevops%'
    OR bizible_referrer_page LIKE '%simplifydevops%'
    OR bizible_ad_campaign_name LIKE '%simplifydevops%'))
    OR camp.campaign_parent_id = '7014M000001doAGQAY'
    OR camp.campaign_id LIKE '7014M000001dn6z%'
    THEN 'Simplify DevOps'
    WHEN  (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%21q4-jp%'
    OR bizible_form_url LIKE '%21q4-jp%'
    OR bizible_referrer_page LIKE '%21q4-jp%'
    OR bizible_ad_campaign_name LIKE '%21q4-jp%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ='2021_Social_Japan_LinkedIn Lead Gen')
    THEN 'Japan-Digital Readiness'
    WHEN  (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%lower-tco%'
    OR bizible_form_url LIKE '%lower-tco%'
    OR bizible_referrer_page LIKE '%lower-tco%'
    OR bizible_ad_campaign_name LIKE '%operationalefficiencies%'
    OR bizible_ad_campaign_name LIKE '%operationalefficiences%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name LIKE '%_Operational Efficiencies%'
        OR bizible_ad_campaign_name LIKE '%operationalefficiencies%'))
    THEN 'Increase Operational Efficiencies'
    WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%reduce-cycle-time%'
    OR bizible_form_url LIKE '%reduce-cycle-time%'
    OR bizible_referrer_page LIKE '%reduce-cycle-time%'
    OR bizible_ad_campaign_name LIKE '%betterproductsfaster%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name LIKE '%_Better Products Faster%'
        OR bizible_ad_campaign_name LIKE '%betterproductsfaster%'))
    THEN 'Deliver Better Products Faster'
    WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%secure-apps%'
    OR bizible_form_url LIKE '%secure-apps%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%secure-apps%'
    OR bizible_ad_campaign_name LIKE '%reducesecurityrisk%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name LIKE '%_Reduce Security Risk%'
        OR bizible_ad_campaign_name LIKE '%reducesecurityrisk%'))
    THEN 'Reduce Security and Compliance Risk'
    WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%jenkins-alternative%'
    OR bizible_form_url LIKE '%jenkins-alternative%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%jenkins-alternative%'
    OR bizible_ad_campaign_name LIKE '%cicdcmp2%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name LIKE '%_Jenkins%'
        OR bizible_ad_campaign_name LIKE '%cicdcmp2%'))
    THEN 'Jenkins Take Out'
    WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%single-application-ci%'
    OR bizible_form_url LIKE '%single-application-ci%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%single-application-ci%'
    OR bizible_ad_campaign_name LIKE '%cicdcmp3%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name LIKE '%cicdcmp3%')
    THEN 'CI Build & Test Auto'
   WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%github-actions-alternative%'
    OR bizible_form_url LIKE '%github-actions-alternative%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%github-actions-alternative%'
    OR bizible_ad_campaign_name LIKE '%octocat%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ILIKE '%_OctoCat%')
    OR  bizible_ad_campaign_name = '20200122_MakingCaseCICD'
    THEN 'OctoCat'
 WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%integration-continue-pour-construire-et-tester-plus-rapidement%'
    OR bizible_form_url LIKE '%integration-continue-pour-construire-et-tester-plus-rapidement%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%integration-continue-pour-construire-et-tester-plus-rapidement%'
    OR (bizible_ad_campaign_name LIKE '%singleappci%' AND BIZIBLE_AD_CONTENT LIKE '%french%')))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ILIKE '%Singleappci_French%')
    THEN 'CI Use Case - FR'
 WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%nutze-continuous-integration-fuer-schnelleres-bauen-und-testen%'
    OR bizible_form_url LIKE '%nutze-continuous-integration-fuer-schnelleres-bauen-und-testen%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%nutze-continuous-integration-fuer-schnelleres-bauen-und-testen%'
    OR (bizible_ad_campaign_name LIKE '%singleappci%' AND BIZIBLE_AD_CONTENT LIKE '%paesslergerman%')))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ILIKE '%Singleappci_German%')
    THEN 'CI Use Case - DE'
 WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%use-continuous-integration-to-build-and-test-faster%'
    OR bizible_form_url LIKE '%use-continuous-integration-to-build-and-test-faster%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%use-continuous-integration-to-build-and-test-faster%'
    OR bizible_ad_campaign_name LIKE '%singleappci%'))
    OR bizible_ad_campaign_name ='20201013_ActualTechMedia_DeepMonitoringCI'
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name LIKE '%_CI%'
        OR bizible_ad_campaign_name ILIKE '%singleappci%'))
   OR (camp.campaign_parent_id = '7014M000001vm9KQAQ' AND bizible_ad_campaign_name LIKE '%_CI%')
    THEN 'CI Use Case' --- Added by AO demand gen issue 2262
 WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%shift-your-security-scanning-left%'
    OR bizible_form_url LIKE '%shift-your-security-scanning-left%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%shift-your-security-scanning-left%'
    OR bizible_ad_campaign_name LIKE '%devsecopsusecase%'))
    OR camp.campaign_parent_id = '7014M000001dnVOQAY' -- GCP Partner campaign
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name ILIKE '%_DevSecOps%'
        OR bizible_ad_campaign_name LIKE '%devsecopsusecase%'))
    OR (camp.campaign_parent_id = '7014M000001vm9KQAQ'
    AND (bizible_ad_campaign_name ILIKE '%DevSecOps%'
    OR bizible_ad_campaign_name LIKE '%Fuzzing%'))-- Added by AO demand gen issue 2262
    THEN 'DevSecOps Use Case'
 WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%aws-gitlab-serverless%'
    OR bizible_landing_page LIKE '%trek10-aws-cicd%'
    OR bizible_form_url LIKE '%aws-gitlab-serverless%'
    OR bizible_form_url LIKE '%trek10-aws-cicd%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%aws-gitlab-serverless%'
    OR bizible_ad_campaign_name LIKE '%awspartner%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ILIKE '%_AWS%')
    THEN 'AWS'
 WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%simplify-collaboration-with-version-control%'
    OR bizible_form_url LIKE '%simplify-collaboration-with-version-control%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%simplify-collaboration-with-version-control%'
    OR bizible_ad_campaign_name LIKE '%vccusecase%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name LIKE '%_VCC%'
        OR bizible_ad_campaign_name LIKE '%vccusecase%'))
    THEN 'VCC Use Case'
 WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%gitops-infrastructure-automation%'
    OR bizible_form_url LIKE '%gitops-infrastructure-automation%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%gitops-infrastructure-automation%'
    OR bizible_ad_campaign_name LIKE '%iacgitops%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name LIKE '%_GitOps%'
        OR bizible_ad_campaign_name LIKE '%iacgitops%'))
    OR (camp.campaign_parent_id = '7014M000001vm9KQAQ' AND bizible_ad_campaign_name ILIKE '%GitOps_%') --- Added by AO demand gen issue 2327
    THEN 'GitOps Use Case'
 WHEN  (bizible_touchpoint_type = 'Web Form'
    AND (bizible_ad_campaign_name LIKE '%evergreen%'
    OR BIZIBLE_FORM_URL_RAW LIKE '%utm_campaign=evergreen%'
    OR BIZIBLE_LANDING_PAGE_RAW LIKE '%utm_campaign=evergreen%'
    OR BIZIBLE_REFERRER_PAGE_RAW LIKE '%utm_campaign=evergreen%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ILIKE '%_Evergreen%')
   THEN 'Evergreen'
 WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_ad_campaign_name LIKE 'brand%'
    OR bizible_ad_campaign_name LIKE 'Brand%'
    OR BIZIBLE_FORM_URL_RAW LIKE '%utm_campaign=brand%'
    OR BIZIBLE_LANDING_PAGE_RAW LIKE '%utm_campaign=brand%'
    OR BIZIBLE_REFERRER_PAGE_RAW LIKE '%utm_campaign=brand%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ILIKE '%_Brand%')
   THEN 'Brand'
WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-06-04 MSandP: 332
    AND (bizible_landing_page LIKE '%contact-us-ultimate%'
    OR bizible_form_url LIKE '%contact-us-ultimate%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%contact-us-ultimate%'
    OR bizible_ad_campaign_name LIKE '%premtoultimatesp%'))
    THEN 'Premium to Ultimate'
WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-06-04 MSandP: 346
    AND ( BIZIBLE_FORM_URL_RAW LIKE '%webcast-gitops-multicloudapp%'
    OR BIZIBLE_LANDING_PAGE_RAW LIKE '%webcast-gitops-multicloudapp%'
    OR BIZIBLE_REFERRER_PAGE_RAW LIKE '%webcast-gitops-multicloudapp%'))
    OR (camp.campaign_parent_id LIKE '%7014M000001dpmf%')
   THEN 'GitOps GTM webcast'
WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-06-04 MSandP: 346
    AND ( BIZIBLE_FORM_URL_RAW LIKE '%devopsgtm%'
    OR BIZIBLE_LANDING_PAGE_RAW LIKE '%devopsgtm%'
    OR BIZIBLE_REFERRER_PAGE_RAW LIKE '%devopsgtm%'))
    OR camp.campaign_parent_id LIKE '%7014M000001dpT9%'
      -- OR camp.campaign_parent_id LIKE '%7014M000001dn8M%')
    OR camp.campaign_id LIKE '%7014M000001vbtw%'
    OR (camp.campaign_parent_id = '7014M000001vm9KQAQ'
    AND (bizible_ad_campaign_name ILIKE '%MLOps%'
    OR bizible_ad_campaign_name ILIKE '%Dora%'
    OR bizible_ad_campaign_name ILIKE '%DevOps%'))-- Added by AO demand gen issue 2262
   THEN 'DevOps GTM'
WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-06-04 MSandP: 346
    AND (( bizible_form_url_raw LIKE '%utm_campaign=devopsgtm%' AND bizible_form_url_raw LIKE '%utm_content=partnercredit%'
    OR bizible_landing_page_raw LIKE '%utm_campaign=devopsgtm%' AND bizible_landing_page_raw LIKE '%utm_content=partnercredit%'
    OR bizible_referrer_page_raw LIKE '%utm_campaign=devopsgtm%' AND bizible_referrer_page_raw LIKE '%utm_content=partnercredit%')
         OR(
       bizible_form_url_raw LIKE '%cloud-credits-promo%'
      OR bizible_landing_page_raw LIKE '%cloud-credits-promo%'
      OR bizible_referrer_page_raw LIKE '%cloud-credits-promo%'
      )))
    OR camp.campaign_parent_id LIKE '%7014M000001vcDr%'
      -- OR camp.campaign_parent_id LIKE '%7014M000001dn8M%')
    OR camp.campaign_id LIKE '%7014M000001vcDr%'
   THEN 'Cloud Partner Campaign'
WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-06-04 MSandP: 346
    AND (( bizible_form_url_raw LIKE '%utm_campaign=gitlab14%'
    OR bizible_landing_page_raw LIKE '%utm_campaign=gitlab14%'
    OR bizible_referrer_page_raw LIKE '%utm_campaign=gitlab14%')
       OR(
      bizible_form_url_raw LIKE '%the-shift-to-modern-devops%'
      OR bizible_landing_page_raw LIKE '%the-shift-to-modern-devops%'
      OR bizible_referrer_page_raw LIKE '%the-shift-to-modern-devops%'
      )))
   THEN 'GitLab 14 webcast'
WHEN
  camp.campaign_id LIKE '%7014M000001drcQ%'
   THEN '20210512_ISSAWebcast'
WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-0830 MSandP: 325
    AND (( bizible_form_url_raw LIKE '%psdigitaltransformation%'
    OR bizible_landing_page_raw LIKE '%psdigitaltransformation%'
    OR bizible_referrer_page_raw LIKE '%psdigitaltransformation%')
       OR(
      bizible_form_url_raw LIKE '%psglobal%'
      OR bizible_landing_page_raw LIKE '%psglobal%'
      OR bizible_referrer_page_raw LIKE '%psglobal%'
      )))
THEN 'PubSec Nurture'
WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-11-22 MSandP: 585
    AND ( bizible_form_url_raw LIKE '%whygitlabdevopsplatform-apac%'
    OR bizible_landing_page_raw LIKE '%whygitlabdevopsplatform-apac%'
    OR bizible_referrer_page_raw LIKE '%whygitlabdevopsplatform-apac%'))
    OR bizible_ad_campaign_name = '20211208_GitHubCompetitive_APAC'
THEN 'FY22 GitHub Competitive Campaign - APAC' --added 2022-04-06 Agnes O Demand Gen issue 2330
WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-11-22 MSandP: 585
    AND (( bizible_form_url_raw LIKE '%whygitlabdevopsplatform%'
    OR bizible_landing_page_raw LIKE '%whygitlabdevopsplatform%'
    OR bizible_referrer_page_raw LIKE '%whygitlabdevopsplatform%')
       OR(
      bizible_form_url_raw LIKE '%githubcompete%'
      OR bizible_landing_page_raw LIKE '%githubcompete%'
      OR bizible_referrer_page_raw LIKE '%githubcompete%'
      )))
   OR bizible_ad_campaign_name = '20211202_GitHubCompetitive'--added 2022-04-06 Agnes O Demand Gen issue 2330
THEN 'FY22 GitHub Competitive Campaign'
WHEN (bizible_touchpoint_type = 'Web Form'
     AND (BIZIBLE_FORM_URL_RAW LIKE '%utm_campaign=cdusecase%'
    OR BIZIBLE_LANDING_PAGE_RAW LIKE '%utm_campaign=cdusecase%'
    OR BIZIBLE_REFERRER_PAGE_RAW LIKE '%utm_campaign=cdusecase%'))
    OR (camp.campaign_parent_id = '7014M000001vm9KQAQ' AND bizible_ad_campaign_name LIKE '%CD_%') --- Added by AO demand gen issue 2262
   THEN 'CD Use Case'
  ELSE 'None'
  END AS bizible_integrated_campaign_grouping,
  IFF(bizible_integrated_campaign_grouping <> 'None','Demand Gen','Other') AS touchpoint_segment,
  CASE
  WHEN bizible_integrated_campaign_grouping IN ('CI Build & Test Auto','CI Use Case','CI Use Case - FR','CI Use Case - DE','CI/CD Seeing is Believing','Jenkins Take Out','OctoCat','Premium to Ultimate','20210512_ISSAWebcast', 'CD Use Case') THEN 'CI/CD'
  WHEN bizible_integrated_campaign_grouping IN ('Deliver Better Products Faster','DevSecOps Use Case','Reduce Security and Compliance Risk','Simplify DevOps', 'DevOps GTM', 'Cloud Partner Campaign', 'GitLab 14 webcast','FY22 GitHub Competitive Campaign') THEN 'DevOps'
  WHEN bizible_integrated_campaign_grouping IN ('GitOps Use Case','GitOps GTM webcast')  THEN 'GitOps'
  ELSE NULL END AS gtm_motion,
    SUM(bizible_count_first_touch)                  AS first_weigh,
    SUM(bizible_count_w_shaped)                     AS w_weigh,
    SUM(bizible_count_u_shaped)                     AS u_weigh,
    SUM(bizible_attribution_percent_full_path)      AS full_weigh,
    COUNT(DISTINCT biz_base.opportunity_id)         AS l_touche,
    (l_touches / count_touches)                     AS l_weigh,
    (biz_base.iacv * first_weight)                  AS first_iac,
    (biz_base.iacv * w_weight)                      AS w_iac,
    (biz_base.iacv * u_weight)                      AS u_iac,
    (biz_base.iacv * full_weight)                   AS full_iac,
    (biz_base.iacv* l_weight)                       AS linear_iacv
FROM
  biz_base
  LEFT JOIN linear_base ON biz_base.opportunity_id = linear_base.opportunity_id
  LEFT JOIN campaigns_per_opp ON biz_base.opportunity_id = campaigns_per_opp.opportunity_id
  LEFT JOIN campaign camp ON biz_base.campaign_id=camp.campaign_id
  LEFT JOIN contacts ON biz_base.bizible_contact=contacts.contact_id
GROUP BY
     1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47

), unioned AS (

SELECT
  tp.bizible_touchpoint_date_normalized         AS bizible_touchpoint_date_normalized,
  tp.bizible_integrated_campaign_grouping       AS bizible_integrated_campaign_grouping,
  tp.bizible_marketing_channel_path             AS bizible_marketing_channel_path,
  tp.bizible_form_url_raw                       AS bizible_form_url_raw,
  tp.bizible_landing_page_raw                   AS bizible_landing_page_raw,
  tp.sfdc_sales_segmentation                    AS sales_segment,
  NULL                                          AS sales_type,
  NULL                                          AS sales_qualified_month,
  NULL                                          AS sales_accepted_date,
  NULL                                          AS stage_name ,
  tp.bizible_medium                             AS bizible_medium,
  tp.bizible_touchpoint_source                  AS bizible_touchpoint_source,
  tp.bizible_ad_campaign_name                   AS bizible_ad_campaign_name,
  tp.bizible_ad_content                         AS bizible_ad_content,
  tp.person_last_utm_campaign                   AS person_last_utm_campaign,
  tp.person_last_utm_content                    AS person_last_utm_content,
  0                                             AS total_cost,
  SUM (tp.Touchpoint_count)                     AS touchpoint_sum,
  SUM (tp.count_mql)                            AS mql_sum,
  SUM (tp.count_accepted)                       AS accepted_sum,
  0                                             AS linear_sao
FROM tp
WHERE tp.bizible_integrated_campaign_grouping <> 'None'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
UNION ALL
SELECT
  ol.bizible_touchpoint_date_normalized         AS opp_touchpoint_date_normalized,
  ol.bizible_integrated_campaign_grouping       AS opp_integrated_campaign_grouping,
  ol.marketing_channel_path                     AS marketing_channel_path,
  ol.bizible_form_url_raw                       AS bizible_form_url_raw,
  ol.bizible_landing_page_raw                   AS bizible_landing_page_raw,
  ol.sales_segment                              AS sales_segment,
  ol.sales_type                                 AS sales_type,
  ol.sales_qualified_month                      AS sales_qualified_month,
  ol.sales_accepted_date                        AS sales_accepted_date,
  ol.stage_name                                 AS stage_name,
  ol.bizible_medium                             AS bizible_medium,
  ol.bizible_touchpoint_source                  AS bizible_touchpoint_source,
  ol.bizible_ad_campaign_name                   AS bizible_ad_campaign_name,
  ol.bizible_ad_content                         AS bizible_ad_content,
  ol.person_last_utm_campaign                   AS person_last_utm_campaign,
  ol.person_last_utm_content                    AS person_last_utm_content,
  0                                             AS total_cost,
  0                                             AS touchpoint_sum,
  0                                             AS mql_sum,
  0                                             AS accepted_sum,
  CASE
  WHEN ol.sales_accepted_date IS NOT NULL THEN SUM(ol.L_WEIGHT)
  ELSE 0 END AS linear_sao
FROM ol
WHERE ol.bizible_integrated_campaign_grouping <> 'None'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16

), final AS (

    SELECT
      bizible_touchpoint_date_normalized                   AS date,
      bizible_integrated_campaign_grouping                 AS bizible_integrated_campaign_grouping,
      sales_segment                                        AS sdfc_sales_segment,
      bizible_ad_campaign_name                             AS bizible_ad_campaign_name,
      bizible_marketing_channel_path                       AS bizible_marketing_channel_path,
      bizible_medium                                       AS bizible_medium,
      bizible_touchpoint_source                            AS bizible_source,
      CASE
        WHEN SPLIT_PART(SPLIT_PART(BIZIBLE_FORM_URL_RAW,'utm_campaign=',2),'&',1)= ''
        THEN SPLIT_PART(SPLIT_PART(BIZIBLE_LANDING_PAGE_RAW,'utm_campaign=',2),'&',1)
      ELSE SPLIT_PART(SPLIT_PART(BIZIBLE_FORM_URL_RAW,'utm_campaign=',2),'&',1) END AS utm_campaign,
      SPLIT_PART(utm_campaign,'_',1)                        AS utm_campaigncode,
      SPLIT_PART(utm_campaign,'_',2)                        AS utm_geo,
      SPLIT_PART(utm_campaign,'_',3)                        AS utm_targeting,
      SPLIT_PART(utm_campaign,'_',4)                        AS utm_ad_unit,
      SPLIT_PART(utm_campaign,'_',5)                        AS "utm_br/bn",
      SPLIT_PART(utm_campaign,'_',6)                        AS utm_matchtype,
      CASE
        WHEN SPLIT_PART(SPLIT_PART(BIZIBLE_FORM_URL_RAW,'utm_content=',2),'&',1)= ''
        THEN SPLIT_PART(SPLIT_PART(BIZIBLE_LANDING_PAGE_RAW,'utm_content=',2),'&',1)
      ELSE SPLIT_PART(SPLIT_PART(BIZIBLE_FORM_URL_RAW,'utm_content=',2),'&',1) END AS utm_content,
      SPLIT_PART(utm_content,'_',1)                         AS utm_contentcode,
      SPLIT_PART(utm_content,'_',2)                         AS utm_team,
      SPLIT_PART(utm_content,'_',3)                         AS utm_segment,
      SPLIT_PART(utm_content,'_',4)                         AS utm_language,
      person_last_utm_campaign                              AS person_last_utm_campaign,
      person_last_utm_content                               AS person_last_utm_content,
      bizible_form_url_raw                                  AS bizible_form_url_raw,
      bizible_landing_page_raw                              AS bizible_landing_page_raw,
      SUM(Touchpoint_sum)                                   AS inquiries,
      SUM(MQL_sum)                                          AS mqls,
      SUM(Accepted_sum)                                     AS sdr_accepted,
      SUM(LINEAR_SAO)                                       AS linear_sao
  FROM unioned
  WHERE DATE >= '2020-02-01'::DATE
  AND bizible_integrated_campaign_grouping <>'None'
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23

)

SELECT *
FROM final
ORDER BY 1 DESC

)

SELECT *
FROM base
ORDER BY DATE DESC

