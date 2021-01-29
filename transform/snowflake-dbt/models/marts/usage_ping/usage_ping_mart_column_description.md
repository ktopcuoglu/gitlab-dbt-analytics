{% docs reporting_month %}	Month of Reporting	{% enddocs %}
{% docs metrics_path%}	unique json expression path of the metrics in the usage ping payload	{% enddocs %}
{% docs ping_id%}	unique identifier of a ping	{% enddocs %}
{% docs host_id%}	Unique Identifier of a host	{% enddocs %}
{% docs license_id%}	Unique Identifier of a license	{% enddocs %}
{% docs license_md5%}	Unique md5 File of a license	{% enddocs %}
{% docs original_linked_subscription_id%}	if not null, the subscription ID currently linked to the license ID	{% enddocs %}
{% docs latest_active_subscription_id%}	If not null, the latest child subscription ID of the subscription linked to the license	{% enddocs %}
{% docs billing_account_id%}	The ID of the Zuora account the subscription is associated with	{% enddocs %}
{% docs location_id%}		{% enddocs %}
{% docs ultimate_parent_account_id%}	ID of the Ultimate Parent Account of the crm_id	{% enddocs %}
{% docs delivery_column %}	Either Self-Managed or SaaS. More info here: https://about.gitlab.com/handbook/marketing/strategic-marketing/tiers/	{% enddocs %}
{% docs main_edition%}	EE vs CE. More info here: https://about.gitlab.com/handbook/marketing/strategic-marketing/tiers/	{% enddocs %}
{% docs ping_product_tier%}	Info infered from the edition and the plan saved in the license. Matches the tier defined here: https://about.gitlab.com/handbook/marketing/strategic-marketing/tiers/	{% enddocs %}
{% docs ping_main_edition_product_tier%}	Concatenation of main_edition and ping_product_tier	{% enddocs %}
{% docs major_version%}	For example, for 13.6.2, major_version is 13. See details here: https://docs.gitlab.com/ee/policy/maintenance.html	{% enddocs %}
{% docs minor_version%}	For example, for 13.6.2, minor_version is 6. See details here: https://docs.gitlab.com/ee/policy/maintenance.html	{% enddocs %}
{% docs major_minor_version%}	Concatenation of major and minor version. For example, for 13.6.2, this is 13.6	{% enddocs %}
{% docs version%}	Version of the GitLab App. See more details here: https://docs.gitlab.com/ee/policy/maintenance.html	{% enddocs %}
{% docs is_pre_release%}	Boolean flag which is set to True if the version is a pre-release Version of the GitLab App. See more details here: https://docs.gitlab.com/ee/policy/maintenance.html	{% enddocs %}
{% docs is_internal%}	Boolean flag which is set to True if the instance has been identified as an internal GitLab instance	{% enddocs %}
{% docs is_trial%}	Boolean flag which is set to True if the instance as a valid trial license at the usage ping creation	{% enddocs %}
{% docs is_staging%}	Boolean flag which is set to True if the instance has been identified as a staging instance	{% enddocs %}
{% docs group_name %}	Name of the group which is responsible for this metric	{% enddocs %}
{% docs stage_name %}	Name of the stage which is responsible for this metric	{% enddocs %}
{% docs section_name %}	Name of the section which is responsible for this metric	{% enddocs %}
{% docs is_smau %}	Boolean flag set to true if the metrics is one of the counters chosen for SMAU calculation 	{% enddocs %}
{% docs is_gmau %}	Boolean flag set to true if the metrics is one of the counters chosen for GMAU calculation	{% enddocs %}
{% docs is_paid_gmau %}	Boolean flag set to true if the metrics is one of the counters chosen for paid GMAU calculation	{% enddocs %}
{% docs is_umau %}	Boolean flag set to true if the metrics is one of the counters chosen for UMAU calculation	{% enddocs %}
{% docs source_ip_hash%}		{% enddocs %}
{% docs host_name%}	Name of the host hosting the GitLab Instance sending	{% enddocs %}
{% docs subscription_name_slugify%}	If a subscription is linked to the license, slugified name of the subscription	{% enddocs %}
{% docs subscription_start_month%}	Month when the Subscription linked to the license started	{% enddocs %}
{% docs subscription_end_month%}	Month when the Subscription linked to the license is supposed to end according to last agreed terms	{% enddocs %}
{% docs product_category_array%}		{% enddocs %}
{% docs product_rate_plan_name_array%}		{% enddocs %}
{% docs is_paid_subscription%}	Boolean flag set to True if the subscription has a ARR > 0	{% enddocs %}
{% docs is_edu_oss_subscription%}	Boolean flag set to True if the subscription is under a EDU/OSS Program	{% enddocs %}
{% docs crm_account_name%}	Account Name coming from SFDC	{% enddocs %}
{% docs ultimate_parent_account_name%}	Ultimate Parent Account Name coming from SFDC	{% enddocs %}
{% docs ultimate_parent_billing_country%}	Billing Country of the Ultimate Parent from SFDC	{% enddocs %}
{% docs ultimate_parent_account_segment%}	Segment of the Ultimate Parent Account from SFDC. Sales Segments are explained here: https://about.gitlab.com/handbook/sales/field-operations/gtm-resources/#segmentation {% enddocs %}
{% docs technical_account_manager %}	Name of the Technical Account Manager of the subscription	{% enddocs %}
{% docs ultimate_parent_industry%}	Industry of the Ultimate Parent Account from SFDC	{% enddocs %}
{% docs ultimate_parent_account_owner_team%}	Owner Team of the Ultimate Parent Account from SFDC	{% enddocs %}
{% docs ultimate_parent_territory%}	Territory of the Ultimate Parent Account from SFDC	{% enddocs %}
{% docs created_at%}	Timestamp when the usage ping payloads has been created	{% enddocs %}
{% docs recorded_at%}	Timestamp when the usage ping payloads has been recorded	{% enddocs %}
{% docs monthly_metric_value%}	On a given month M, value for the metric path on a specific instance	{% enddocs %}
{% docs created_by%}		{% enddocs %}
{% docs updated_by%}		{% enddocs %}
{% docs model_created_date%}		{% enddocs %}
{% docs model_updated_date%}		{% enddocs %}
{% docs dbt_updated_at%}		{% enddocs %}
{% docs dbt_created_at%}		{% enddocs %}
