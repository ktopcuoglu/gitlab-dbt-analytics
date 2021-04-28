SELECT *
FROM  {{ ref('pump_marketing_contact') }}
WHERE (
  individual_namespace_is_saas_premium_tier
  OR group_owner_of_saas_premium_tier
  OR group_member_of_saas_premium_tier
  OR responsible_for_group_saas_premium_tier
  )
AND gitlab_dotcom_email_opted_in = true
OR is_self_managed_premium_tier
