{% docs mart_event %}

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

**Description:** Enriched GitLab.com Usage Event Data for Valid Events
- [Targets and Actions](https://docs.gitlab.com/ee/api/events.html) activity by Users and [Namespaces](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/namespace/) within the GitLab.com application are captured and refreshed periodically throughout the day.  Targets are objects ie. issue, milestone, merge_request and Actions have effect on Targets, ie. approved, closed, commented, created, etc.  These events are captured from the GitLab application.
- The data is Enriched with Extra business related attributes for Namespace, User and Project to allow single table queries that satisfy a Larger Generalized set of Use Cases. 

**Data Grain:**
- event_id
- event_created_at

**Filters:**
- Use Valid Events Only for standard analysis and reporting:
  - Remove Events where the Event Created Datetime < the User Created Datetime.
    - These are usually events from projects that were created before the User and then imported in by the User after the User is created.  
  - Keep Events where User Id = NULL.  These do not point to a particular User, ie. 'milestones' 
- Rolling 24mos of Data  

**Business Logic in this Model:** 
- Event, User and Ultimate_Namespace counts are included for the Aggregation Level

**Other Comments:**
- The `mart_event_with_valid_user` table is built directly from the [fct_event_with_valid_user](https://gitlab-data.gitlab.io/analytics/#!/model/model.gitlab_snowflake.fct_event_with_valid_user) which brings all of the different types of events together.  
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}

{% docs mart_event_namespace_daily %}

**Description:** Enriched GitLab.com Usage Event Data Grouped by Date, Event, Namespace and Billing for Valid Events with extra Namespace Attributes
- [Targets and Actions](https://docs.gitlab.com/ee/api/events.html) activity by Users and [Namespaces](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/namespace/) within the GitLab.com application are captured and refreshed periodically throughout the day.  Targets are objects ie. issue, milestone, merge_request and Actions have effect on Targets, ie. approved, closed, commented, created, etc.  These events are captured from the GitLab application.
- The data is aggregated by Date, Event and Namespace and includes supporting Attributes with Enhanced Namespace Attributes. 

**Data Grain:**
- event_date
- event_name
- dim_ultimate_parent_namespace_id

**Filters:**
- Use Valid Events Only for standard analysis and reporting:
  - Remove Events where the Event Created Datetime < the User Created Datetime.
    - These are usually events from projects that were created before the User and then imported in by the User after the User is created.  
  - Keep Events where User Id = NULL.  These do not point to a particular User, ie. 'milestones' 
- Rolling 24mos of Data  

**Business Logic in this Model:** 
- The Actual Ultimate Parent Namespace, Plan, Subscription, Billing and Product Information for the Event is determined by the Event Date.
- Each Event is identified as being used for different xMAU metrics (is_smau, is_gmau, is_umau)
- `data_source` = 'GITLAB_DOTCOM'

**Other Comments:**
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}

{% docs mart_event_daily %}

**Description:** Enriched GitLab.com Usage Event Data with Only Valid Events by Event_Date, User, Ultimate_Parent_Namespace and Event_Name with extra Namespace Attributes
- [Targets and Actions](https://docs.gitlab.com/ee/api/events.html) activity by Users and [Namespaces](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/namespace/) within the GitLab.com application are captured and refreshed periodically throughout the day.  Targets are objects ie. issue, milestone, merge_request and Actions have effect on Targets, ie. approved, closed, commented, created, etc.  These events are captured from the GitLab application.
- The data is aggregated by Event_Date, User, Ultimate_Parent_Namespace and Event_Name and includes supporting Attributes with Enhanced Namespace Attributes.

**Data Grain:**
- event_date
- dim_user_id
- dim_ultimate_parent_namespace_id
- event_name

**Filters:**
- Use ONLY Valid Events for standard analysis and reporting:
  - Remove Events where the Event Created Datetime < the User Created Datetime.
    - These are usually events from projects that were created before the User and then imported in by the User after the User is created.  
  - Keep Events where User Id = NULL.  These do not point to a particular User, ie. 'milestones' 
- Rolling 24mos of Data  

**Business Logic in this Model:** 

**Other Comments:**
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}
