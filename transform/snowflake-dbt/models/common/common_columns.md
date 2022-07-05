{% docs event_id %}

The unique identifier of an event. This is a generated primary key and will not join back to the source models

{% enddocs %}

{% docs dim_active_product_tier_id %}

The unique identifier of the ultimate parent namespace's latest product tier, easily joined to `dim_product_tier`

{% enddocs %}

{% docs dim_active_subscription_id %}

The unique identifier of the ultimate parent namespace's latest subscription, easily joined to `dim_subscription`

{% enddocs %}

{% docs dim_event_date_id %}

The ID of the event date, easily joined to `dim_date`

{% enddocs %}

{% docs dim_crm_account_id %}

The unique identifier of a crm account, easily joined to `dim_crm_account`

{% enddocs %}

{% docs dim_billing_account_id %}

The identifier of the Zuora account associated with the subscription, easily joined to `dim_billing_account`

{% enddocs %}

{% docs dim_ultimate_parent_namespace_id %}

The unique identifier of the ultimate parent namespace in which the event was generated, easily joined to `dim_namespace`. The recommended JOIN is `dim_ultimate_parent_namespace_id = dim_namespace.dim_namespace_id`, which will be a one-to-one relationship. JOINing on `dim_ultimate_parent_namespace_id = dim_namespace.ultimate_parent_namespace_id` will return `dim_namespace` records for both the ultimate parent _and_ all sub-groups underneath it. This field will be NULL if the event is not tied to a namespace (ex. users_created)

{% enddocs %}

{% docs dim_project_id %}

The unique identifier of the project in which the event was generated, easily joined to `dim_project`. This will be NULL if the event is not tied to a project (ex. epic_creation, etc)

{% enddocs %}

{% docs dim_user_id %}

The unique identifier of the user who generated the event, easily joined to `dim_user`. This will be NULL if the event is not tied to a specific user (ex. terraform_reports, etc)

{% enddocs %}

{% docs event_created_at %}

Timestamp of the event

{% enddocs %}

{% docs event_date %}

The date of the event

{% enddocs %}

{% docs parent_id %}

The unique identifier of the project (dim_project_id) associated with the event. If no project is associated, the ultimate parent namespace associated with the event. This will be NULL if neither a project or namespace is associated with the event

{% enddocs %}

{% docs parent_type %}

Denotes whether the event was associate with a project or namespace ('project' or 'group'). This will be NULL if neither a project or namespace is associated with the event

{% enddocs %}

{% docs is_smau %}

Boolean flag set to True if the event is chosen for the stage's SMAU metric

{% enddocs %}

{% docs is_gmau %}

Boolean flag set to True if the event is chosen for the group's GMAU metric

{% enddocs %}

{% docs is_umau %}

Boolean flag set to True if the event is chosen for the UMAU metric

{% enddocs %}

{% docs event_name %}

The name tied to the event

{% enddocs %}

{% docs stage_name %}

The name of the product stage (ex. secure, plan, create, etc)

{% enddocs %}

{% docs section_name %}

The name of the product section (ex. dev, ops, etc)

{% enddocs %}

{% docs group_name %}

The name of the product group (ex. code_review, project_management, etc)

{% enddocs %}

{% docs plan_id_at_event_date %}

The ID of the ultimate parent namespace's plan on the day the event was created (ex. 34, 100, 101, etc). If multiple plans are available on a single day, this reflects the last available plan for the namespace. Defaults to '34' (free) if a value is not available

{% enddocs %}

{% docs plan_name_at_event_date %}

The name of the ultimate parent namespace's plan type on the day when the event was created (ex. free, premium, ultimate). If multiple plans are available on a single day, this reflects the last available plan for the namespace. Defaults to 'free' if a value is not available

{% enddocs %}

{% docs plan_was_paid_at_event_date %}

Boolean flag which is set to True if the ultimate parent namespace's plan was paid on the day when the event was created. If multiple plans are available on a single day, this reflects the last available plan for the namespace. Defaults to False if a value is not available

{% enddocs %}

{% docs plan_id_at_event_timestamp %}

The ID of the ultimate parent namespace's plan at the timestamp the event was created (ex. 34, 100, 101, etc). Defaults to '34' (free) if a value is not available

{% enddocs %}

{% docs plan_name_at_event_timestamp %}

The name of the ultimate parent namespace's plan type at the timestamp when the event was created (ex. free, premium, ultimate). Defaults to 'free' if a value is not available

{% enddocs %}

{% docs plan_was_paid_at_event_timestamp %}

Boolean flag which is set to True if the ultimate parent namespace's plan was paid at the timestamp when the event was created. Defaults to False if a value is not available

{% enddocs %}

{% docs days_since_user_creation_at_event_date %}

The count of days between user creation and the event. This will be NULL if a user is not associated with the event

{% enddocs %}

{% docs days_since_namespace_creation_at_event_date %}

The count of days between ultimate parent namespace creation and the event. This will be NULL if a namespace is not associated with the event

{% enddocs %}

{% docs days_since_project_creation_at_event_date %}

The count of days between project creation and the event. This will be NULL if a project is not associated with the event

{% enddocs %}

{% docs data_source %}

The source application where the data was extracted from (ex. GITLAB_DOTCOM)

{% enddocs %}

{% docs namespace_is_internal %}

Boolean flag set to True if the ultimate parent namespace in which the event was generated is identified as an internal GitLab namespace

{% enddocs %}

{% docs namespace_creator_is_blocked %}

Boolean flag set to True if the ultimate parent namespace creator is in a 'blocked' or 'banned' state

{% enddocs %}

{% docs namespace_created_at %}

The timestamp of the ultimate parent namespace creation

{% enddocs %}

{% docs namespace_created_date %}

The date of the ultimate parent namespace creation

{% enddocs %}

{% docs user_created_at %}

The timestamp of the user creation

{% enddocs %}

{% docs is_blocked_user %}

Boolean flag set to True if the user who generated the events is in a 'blocked' or 'banned' state

{% enddocs %}

{% docs project_is_learn_gitlab %}

Boolean flag set to True if the project in which the event was generated was a Learn GitLab project, one automatically created during user onboarding

{% enddocs %}

{% docs project_is_imported %}

Boolean flag set to True if the project in which the event was generated was imported

{% enddocs %}

{% docs event_calendar_month %}

The first day of the calendar month of the event (ex. 2022-05-01, etc)

{% enddocs %}

{% docs event_calendar_quarter %}

The calendar quarter of the event (ex. 2022-Q2, etc)

{% enddocs %}

{% docs event_calendar_year %}

The calendar year of the event (ex. 2022, etc)

{% enddocs %}

{% docs created_by %}

The GitLab handle of the original model creator

{% enddocs %}

{% docs updated_by %}

The GitLab handle of the most recent model editor

{% enddocs %}

{% docs model_created_date %}

Manually input ISO date of when model was original created

{% enddocs %}

{% docs model_updated_date %}

Manually input ISO date of when model was updated

{% enddocs %}

{% docs event_count %}

The count of events generated

{% enddocs %}

{% docs user_count %}

 The count of distinct users who generated an event

{% enddocs %}

{% docs ultimate_parent_namespace_count %}

 The count of distinct ultimate parent namespaces in which an event was generated

{% enddocs %}