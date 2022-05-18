{% docs rpt_event_xmau_metric_monthly %}
Reporting model that calculates unique user and namespace counts for GitLab.com xMAU metrics.

Type of Data: gitlab.com db usage events

Aggregate Grain: user_group (total, free, paid), section_name, stage_name, and group_name

Time Grain: reporting_month (defined as the last 28 days of the calendar month). This is intended to match the instance-level service ping metrics by getting a 28-day count of each event.

Use case: Paid SaaS xMAU, SaaS SpO

Note: Usage is attributed to a namespace's last reported plan (free vs paid)
{% enddocs %}

{% docs rpt_event_plan_monthly %}

**Description:** GitLab.com Usage Event Report Data with Monthly Totals for Valid Events
- [Targets and Actions](https://docs.gitlab.com/ee/api/events.html) activity by Users and [Namespaces](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/namespace/) within the GitLab.com application are captured and refreshed periodically throughout the day.  Targets are objects ie. issue, milestone, merge_request and Actions have effect on Targets, ie. approved, closed, commented, created, etc.  These events are captured from the GitLab application.

**Data Grain:**
- event_calendar_month
- event_name
- plan_id_at_event_date

**Filters:**
- Use Valid Events Only for standard analysis and reporting:
  - Remove Events where the Event Created Datetime < the User Created Datetime.
    - These are usually events from projects that were created before the User and then imported in by the User after the User is created.  
  - Keep Events where User Id = NULL.  These do not point to a particular User, ie. 'milestones' 
- Rolling 24mos of Data
- Eliminate rows where Event_Date is Less than 28 days from the Last Day of the Month the Event_Date is in  

**Business Logic in this Model:** 
- Counts are based on the Event Date being within the Last Day of the Month and 27 days prior to the Last Day of the Month (total 28 days)
  - Events that are 29,30 or 31 days prior to the Last Day of the Month will Not be included in these totals
  - This is intended to match the instance-level service ping metrics by getting a 28-day count

**Other Comments:**
- The `mart_event_with_valid_user` table is built directly from the [fct_event_with_valid_user](https://gitlab-data.gitlab.io/analytics/#!/model/model.gitlab_snowflake.fct_event_with_valid_user) which brings all of the different types of events together.  
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}
