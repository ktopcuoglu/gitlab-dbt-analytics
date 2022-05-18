{% docs mart_event %}
Enriched version of the atomic (event-level) GitLab.com usage events table, `common.fct_event`

Type of Data: gitlab.com db usage events

Aggregate Grain: None

Time Grain: None

Use case: Everyday analysis and dashboards; flexibility in aggregating by sets of events, different time ranges, exclude specific projects, etc

Note: This model includes events occurring before a gitlab.com user was created (ex: imported projects; see fct_event for more details). Events not tied to a specific user are included.

**Description:** Enriched Atomic level GitLab.com Usage Event Data 
- [Targets and Actions](https://docs.gitlab.com/ee/api/events.html) activity by Users and [Namespaces](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/namespace/) within the GitLab.com application are captured and refreshed periodically throughout the day.  Targets are objects ie. issue, milestone, merge_request and Actions have effect on Targets, ie. approved, closed, commented, created, etc.  These events are captured from the GitLab application.
- The data is Enriched with Extra business related attributes for Namespace, User and Project to allow single table queries that satisfy a Larger Generalized set of Use Cases. 

**Data Grain:**
- event_id
- event_created_at

**Filters:**
- None - `ALL Data` at the Atomic (`lowest level/grain`) is brought through from the Source for comprehensive analysis.  
  - Futher filters may be needed for Standard Analysis and Reporting, ie. Limiting to Events with Valid Users  

**Business Logic in this Model:** 
- The Actual Ultimate Parent Namespace, Plan, Subscription, Billing and Product Information for the Event is determined by the Event Date.
- Each Event is identified as being used for different xMAU metrics (is_smau, is_gmau, is_umau)
- `data_source` = 'GITLAB_DOTCOM'

**Other Comments:**
- The `mart_event` table is built directly from the [fct_event table](https://gitlab-data.gitlab.io/analytics/#!/model/model.gitlab_snowflake.fct_event)
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}

{% docs mart_event_with_valid_user %}
Enriched version of the derived (event-level) `common.fct_event_with_valid_user` GitLab.com usage events table which filters out invalid users and provides a rolling 2 years of data. 

Type of Data: gitlab.com db usage events

Aggregate Grain: None

Time Grain: None

Use case: Everyday analysis and dashboards; flexibility in aggregating by sets of events, different time ranges, exclude specific projects, etc

Note: This model excludes events occurring before a gitlab.com user was created (ex: imported projects; see fct_event_with_valid_user for more details). Events not tied to a specific user are included.

{% enddocs %}

{% docs mart_event_namespace_daily %}
Enhanced version of `common.fct_event_namespace_daily`

Type of Data: gitlab.com db usage events

Aggregate Grain: event_name, dim_ultimate_parent_namespace_id

Time Grain: event_date

Use case: everyday analysis and dashboards; flexibility in aggregating by sets of events, different time ranges

Note: This model excludes events occurring before a gitlab.com user was created (ex: imported projects; see fct_event for more details). Events not tied to a specific user are included.

{% enddocs %}

{% docs mart_event_daily %}
Enhanced version of `common.fct_event_daily`

Type of Data: gitlab.com db usage events

Aggregate Grain: event_name, dim_ultimate_parent_namespace_id, dim_user_id

Time Grain: event_date

Use case: everyday analysis and dashboards; flexibility in aggregating by sets of events, different time ranges

Note: This model excludes events occurring before a gitlab.com user was created (ex: imported projects; see fct_event for more details). Events not tied to a specific user are excluded.

{% enddocs %}
