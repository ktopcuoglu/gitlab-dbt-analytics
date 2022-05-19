{% docs rpt_event_xmau_metric_monthly %}

**Description:** GitLab.com Usage Event Report Data with Monthly Totals for Valid Free and Paid Events for User Type Events that are Used in xMAU Metrics
- [Targets and Actions](https://docs.gitlab.com/ee/api/events.html) activity by Users and [Namespaces](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/namespace/) within the GitLab.com application are captured and refreshed periodically throughout the day.  Targets are objects ie. issue, milestone, merge_request and Actions have effect on Targets, ie. approved, closed, commented, created, etc.  

**Data Grain:**
- event_calendar_month
- user_group
- section_name
- stage_name
- group_name (plan_was_paid_at_event_date)

**Filters:**
- Use Valid Events Only for standard analysis and reporting:
  - Remove Events where the Event Created Datetime < the User Created Datetime.
    - These are usually events from projects that were created before the User and then imported in by the User after the User is created.  
  - Keep Events where User Id = NULL.  These do not point to a particular User, ie. 'milestones' 
- Rolling 24mos of Data
- Include rows where the Event_Date is within 28 days of the Last Day of the Month
- Include User Type Events 
- Include Events used in Metrics (umau, gmau, smau)  

**Business Logic in this Model:** 
- Valid events where the Event Create DateTime is >= User Create DateTime
- Aggregated Counts are based on the Event Date being within the Last Day of the Month and 27 days prior to the Last Day of the Month (total 28 days)
  - Events that are 29,30 or 31 days prior to the Last Day of the Month will Not be included in these totals
  - This is intended to match the instance-level service ping metrics by getting a 28-day count
- The Last Plan Id of the Month for the Namespace is used for the Calculations and Reporting.

**Other Comments:**
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}

{% docs rpt_event_plan_monthly %}

**Description:** GitLab.com Usage Event Report Data with Monthly Totals for Valid Events
- [Targets and Actions](https://docs.gitlab.com/ee/api/events.html) activity by Users and [Namespaces](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/namespace/) within the GitLab.com application are captured and refreshed periodically throughout the day.  Targets are objects ie. issue, milestone, merge_request and Actions have effect on Targets, ie. approved, closed, commented, created, etc.  

**Data Grain:**
- event_calendar_month
- event_name
- user_group (plan_was_paid_at_event_date)

**Filters:**
- Use Valid Events Only for standard analysis and reporting:
  - Remove Events where the Event Created Datetime < the User Created Datetime.
    - These are usually events from projects that were created before the User and then imported in by the User after the User is created.  
  - Keep Events where User Id = NULL.  These do not point to a particular User, ie. 'milestones' 
- Rolling 24mos of Data
- Include rows where the Event_Date is within 28 days of the Last Day of the Month  

**Business Logic in this Model:** 
- Valid events where the Event Create DateTime is >= User Create DateTime
- Aggregated Counts are based on the Event Date being within the Last Day of the Month and 27 days prior to the Last Day of the Month (total 28 days)
  - Events that are 29,30 or 31 days prior to the Last Day of the Month will Not be included in these totals
  - This is intended to match the instance-level service ping metrics by getting a 28-day count

**Other Comments:**
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}
