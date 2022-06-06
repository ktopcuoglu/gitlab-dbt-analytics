{% macro gtm_motion() %}

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
    OR bizible_referrer_page LIKE '%secure-apps%'
    OR bizible_ad_campaign_name LIKE '%reducesecurityrisk%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name LIKE '%_Reduce Security Risk%'
        OR bizible_ad_campaign_name LIKE '%reducesecurityrisk%'))
    THEN 'Reduce Security and Compliance Risk'
    WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%jenkins-alternative%'
    OR bizible_form_url LIKE '%jenkins-alternative%'
    OR bizible_referrer_page LIKE '%jenkins-alternative%'
    OR bizible_ad_campaign_name LIKE '%cicdcmp2%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name LIKE '%_Jenkins%'
        OR bizible_ad_campaign_name LIKE '%cicdcmp2%'))
    THEN 'Jenkins Take Out'
    WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%single-application-ci%'
    OR bizible_form_url LIKE '%single-application-ci%'
    OR bizible_referrer_page LIKE '%single-application-ci%'
    OR bizible_ad_campaign_name LIKE '%cicdcmp3%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name LIKE '%cicdcmp3%')
    THEN 'CI Build & Test Auto'
   WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%github-actions-alternative%'
    OR bizible_form_url LIKE '%github-actions-alternative%'
    OR bizible_referrer_page LIKE '%github-actions-alternative%'
    OR bizible_ad_campaign_name LIKE '%octocat%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ILIKE '%_OctoCat%')
    OR  bizible_ad_campaign_name = '20200122_MakingCaseCICD'
    THEN 'OctoCat'
 WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%integration-continue-pour-construire-et-tester-plus-rapidement%'
    OR bizible_form_url LIKE '%integration-continue-pour-construire-et-tester-plus-rapidement%'
    OR bizible_referrer_page LIKE '%integration-continue-pour-construire-et-tester-plus-rapidement%'
    OR (bizible_ad_campaign_name LIKE '%singleappci%' AND BIZIBLE_AD_CONTENT LIKE '%french%')))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ILIKE '%Singleappci_French%')
    THEN 'CI Use Case - FR'
 WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%nutze-continuous-integration-fuer-schnelleres-bauen-und-testen%'
    OR bizible_form_url LIKE '%nutze-continuous-integration-fuer-schnelleres-bauen-und-testen%'
    OR bizible_referrer_page LIKE '%nutze-continuous-integration-fuer-schnelleres-bauen-und-testen%'
    OR (bizible_ad_campaign_name LIKE '%singleappci%' AND BIZIBLE_AD_CONTENT LIKE '%paesslergerman%')))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ILIKE '%Singleappci_German%')
    THEN 'CI Use Case - DE'
 WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%use-continuous-integration-to-build-and-test-faster%'
    OR bizible_form_url LIKE '%use-continuous-integration-to-build-and-test-faster%'
    OR bizible_referrer_page LIKE '%use-continuous-integration-to-build-and-test-faster%'
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
    OR bizible_referrer_page LIKE '%shift-your-security-scanning-left%'
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
    OR bizible_referrer_page LIKE '%aws-gitlab-serverless%'
    OR bizible_ad_campaign_name LIKE '%awspartner%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ILIKE '%_AWS%')
    THEN 'AWS'
 WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%simplify-collaboration-with-version-control%'
    OR bizible_form_url LIKE '%simplify-collaboration-with-version-control%'
    OR bizible_referrer_page LIKE '%simplify-collaboration-with-version-control%'
    OR bizible_ad_campaign_name LIKE '%vccusecase%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name LIKE '%_VCC%'
        OR bizible_ad_campaign_name LIKE '%vccusecase%'))
    THEN 'VCC Use Case'
 WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%gitops-infrastructure-automation%'
    OR bizible_form_url LIKE '%gitops-infrastructure-automation%'
    OR bizible_referrer_page LIKE '%gitops-infrastructure-automation%'
    OR bizible_ad_campaign_name LIKE '%iacgitops%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name LIKE '%_GitOps%'
        OR bizible_ad_campaign_name LIKE '%iacgitops%'))
    OR (camp.campaign_parent_id = '7014M000001vm9KQAQ' AND bizible_ad_campaign_name ILIKE '%GitOps_%') --- Added by AO demand gen issue 2327
    THEN 'GitOps Use Case'
 WHEN  (bizible_touchpoint_type = 'Web Form'
    AND (bizible_ad_campaign_name LIKE '%evergreen%'
    OR bizible_form_url_raw LIKE '%utm_campaign=evergreen%'
    OR bizible_landing_page_raw LIKE '%utm_campaign=evergreen%'
    OR bizible_referrer_page_RAW LIKE '%utm_campaign=evergreen%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ILIKE '%_Evergreen%')
   THEN 'Evergreen'
 WHEN (bizible_touchpoint_type = 'Web Form'
    AND (bizible_ad_campaign_name LIKE 'brand%'
    OR bizible_ad_campaign_name LIKE 'Brand%'
    OR bizible_form_url_raw LIKE '%utm_campaign=brand%'
    OR bizible_landing_page_raw LIKE '%utm_campaign=brand%'
    OR bizible_referrer_page_RAW LIKE '%utm_campaign=brand%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ILIKE '%_Brand%')
   THEN 'Brand'
 WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-04-27 MSandP: 316
    AND (bizible_ad_campaign_name LIKE 'simplifydevops%'
   OR bizible_ad_campaign_name LIKE 'Simplifydevops%'
    OR bizible_form_url_raw LIKE '%utm_campaign=simplifydevops%'
    OR bizible_landing_page_raw LIKE '%utm_campaign=simplifydevops%'
    OR bizible_referrer_page_RAW LIKE '%utm_campaign=simplifydevops%'))
    OR (camp.campaign_parent_id = '7014M000001dn6zQAA')
   THEN 'Simplify DevOps'
WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-06-04 MSandP: 332
    AND (bizible_landing_page LIKE '%contact-us-ultimate%'
    OR bizible_form_url LIKE '%contact-us-ultimate%'
    OR bizible_referrer_page LIKE '%contact-us-ultimate%'
    OR bizible_ad_campaign_name LIKE '%premtoultimatesp%'))
    THEN 'Premium to Ultimate'
WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-06-04 MSandP: 346
    AND ( bizible_form_url_raw LIKE '%webcast-gitops-multicloudapp%'
    OR bizible_landing_page_raw LIKE '%webcast-gitops-multicloudapp%'
    OR bizible_referrer_page_RAW LIKE '%webcast-gitops-multicloudapp%'))
    OR (camp.campaign_parent_id LIKE '%7014M000001dpmf%')
   THEN 'GitOps GTM webcast'
WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-06-04 MSandP: 346
    AND ( bizible_form_url_raw LIKE '%devopsgtm%'
    OR bizible_landing_page_raw LIKE '%devopsgtm%'
    OR bizible_referrer_page_RAW LIKE '%devopsgtm%'))
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
     AND (bizible_form_url_raw LIKE '%utm_campaign=cdusecase%'
    OR bizible_landing_page_raw LIKE '%utm_campaign=cdusecase%'
    OR bizible_referrer_page_RAW LIKE '%utm_campaign=cdusecase%'))
    OR (camp.campaign_parent_id = '7014M000001vm9KQAQ' AND bizible_ad_campaign_name LIKE '%CD_%') --- Added by AO demand gen issue 2262
   THEN 'CD Use Case'
  ELSE 'None'
  END AS bizible_integrated_campaign_grouping,
  IFF(bizible_integrated_campaign_grouping <> 'None','Demand Gen','Other') AS touchpoint_segment,
  CASE
  WHEN bizible_integrated_campaign_grouping IN ('CI Build & Test Auto','CI Use Case','CI Use Case - FR','CI Use Case - DE','CI/CD Seeing is Believing','Jenkins Take Out','OctoCat','Premium to Ultimate','20210512_ISSAWebcast', 'CD Use Case') THEN 'CI/CD'
  WHEN bizible_integrated_campaign_grouping IN ('Deliver Better Products Faster','DevSecOps Use Case','Reduce Security and Compliance Risk','Simplify DevOps', 'DevOps GTM', 'Cloud Partner Campaign', 'GitLab 14 webcast','FY22 GitHub Competitive Campaign') THEN 'DevOps'
  WHEN bizible_integrated_campaign_grouping IN ('GitOps Use Case','GitOps GTM webcast')  THEN 'GitOps'
  ELSE NULL END

{% endmacro %}