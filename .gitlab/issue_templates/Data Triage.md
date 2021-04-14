## Data Triage 

<!--
Please complete all items. Ask questions in the #data slack channel
--->

## Housekeeping 
* [ ] Title the issue as "<ISO date> Data Triage" e.g. 2020-07-09 Data Triage
* [ ] Assign this issue to both the Data Analyst and Data Engineer assigned to Triage 
* [ ] [Add a weight to the issue](https://about.gitlab.com/handbook/business-ops/data-team/how-we-work/#issue-pointing)
* [ ] Link any issues opened as a result of Data Triage to this `parent` issue. 

## Data Analyst tasks
All tasks below should be checked off on your Triage day. 
This is the most important issue on your Triage day. 
Please prioritize this issue since we dedicate a day from your milestone to this activity. 

### Reply to slack channels 
* [ ] Review each slack message request in the **#data** and **#data-lounge** channel 
    - [ ] Reply to slack threads by pointing GitLab team member to the appropriate handbook page, visualization, or to other GitLab team member who may know more about the topic. 
    - [ ] Direct GitLab team member to the channel description, which has the link to the Data team project, if the request requires more than 5 minutes of investigative effort from a Data team member.
* [ ] Review each slack message in the **#data-triage** channel, which will inform the triager of what issues have been opened in the data team project that day.  Because this channel can sometimes be difficult to keep track of, you **should** also look at [issues with the ~"Needs Triage" label](https://gitlab.com/gitlab-data/analytics/-/issues?label_name%5B%5D=Needs+Triage&scope=all&state=opened), as this label is added every hour to issues that may have been missed.
    - [ ] For each issue opened by a non-Data Team member, label the issue by: 
        - [ ] Adding the `Workflow::start (triage)` and `Triage` label
        - [ ] Adding additional [labels](https://about.gitlab.com/handbook/business-ops/data-team/how-we-work/#issue-labeling)
        - [ ] Assigning the issue based on:
            - [ ] the [CODEOWNERS file](https://gitlab.com/gitlab-data/analytics/blob/master/CODEOWNERS) for specific dbt model failures 
            - [ ] the [functional DRIs](https://about.gitlab.com/handbook/business-ops/data-team/organization/#team-organization)
            - [ ] OR to the  Manager, Data if you aren't sure. 
        - [ ] Asking initial questions (data source, business logic clarification, etc) to groom the issue. 

### Maintain KPI related information         
* [ ] Maintain the KPI Index page by 
    - [ ] Creating an issue with any outstanding concerns from your respective division (broken links, missing KPI definitions, charts vs links, etc)
    - [ ] Assigning the issue to the [functional DRIs](https://about.gitlab.com/handbook/business-ops/data-team/organization/#team-organization)
* [ ] Review the commit history of the following two files and update the [sheetload_kpi_status data](https://docs.google.com/spreadsheets/d/1CZLnXiAG7D_T_6vm50X0hDPnMPKrKmtajrcga5vyDTQ/edit?usp=sharing) with any new KPIs or update existing KPI statistics (`commit_start` column is the commit URL for the `start_date` and `commit_handbook_v1` column is the commit URL for the `completion_date`)
    - [ ] [KPI Index handbook page](https://gitlab.com/gitlab-com/www-gitlab-com/-/commits/master/source/handbook/business-ops/data-team/kpi-index/index.html.md.erb)
    - [ ] [Engineering KPI list](https://gitlab.com/gitlab-com/www-gitlab-com/-/blob/master/data/performance_indicators.yml)

### Prepare for Next Milestone 
* [ ] Groom Issues for Next Milestone: for issues that have missing or poor requirements, add comments in the issue asking questions to the Business DRI. 
* [ ] Update the planning issues for this milestone and the next milestone 
* [ ] Close/clean-up any irrelevant issues in your backlog. 


## Data Engineer tasks

* [ ] Notify Data Customers of [data refresh SLO](https://about.gitlab.com/handbook/business-ops/data-team/platform/#extract-and-load) breach by posting a message to the _#data_ Slack channel using the appropriate Data Notification Template
* [ ] [Create an issue](https://gitlab.com/gitlab-data/analytics/issues/new?issuable_template=DE%20Triage%20Errors) for each new failure in **#data-prom-alerts**
    * [ ] Link to all resulting issues and MRs in slack 
* [ ] [Create an issue](https://gitlab.com/gitlab-data/analytics/issues/new?issuable_template=DE%20Triage%20Errors) for each new failure in **#dbt-runs** 
    * [ ] Link to all resulting issues and MRs in slack 
* [ ] [Create an issue](https://gitlab.com/gitlab-data/analytics/issues/new?issuable_template=DE%20Triage%20Errors) for each new failure in **#analytics-pipelines**
    * [ ] Link to all resulting issues and MRs in slack 
* [ ] Investigate all pings to the `@gitlab-data/engineers` group from merge requests to the gitlab.com database schema.  Link each MR reviewed to this issue.

In addition to these tasks the Data Engineer on triage should be focused on resolving these issues, including the backlog found on the [DE - Triage Errors board](https://gitlab.com/groups/gitlab-data/-/boards/1917859)


### Data Notification Templates

Use these to notify stakeholders of Data Delays.

<details>
<summary><i>Data Source Delay Templates</i></summary>

Post notices to #data and cross-post to #whats-happening-at-gitlab

#### GitLab.com

We have identified a delay in the `GitLab.com` data refresh and this problem potentially also delays data for GitLab KPIs (e.g. MR Rate, TMAU) or SiSense dashboards. We are actively working on a resolution and will provide an update once the KPIs and SiSense dashboards have been brought up-to-date.

The `GitLab.com` data in the warehouse and downstream models is accurate as of YYYY-MM-DD HH:MM UTC (HH:MM PST).

The DRI for this incident is `@username`.

The link to the Data Team Incident issue is <link>

Replication lag to the database we extract from can be monitored by [checking Thanos](https://thanos-query.ops.gitlab.net/graph?g0.range_input=2w&g0.max_source_resolution=0s&g0.expr=pg_replication_lag%7Btype%3D%22postgres-archive%22%2Cenv%3D%22gprd%22%7D&g0.tab=0&g1.range_input=3d&g1.max_source_resolution=0s&g1.expr=rate(pg_xlog_position_bytes%7Benv%3D%22gprd%22%7D%5B1m%5D)%20and%20on%20(instance)%20(pg_replication_is_replica%20%3D%3D%200)&g1.tab=0)

`CC @Mek Stittri, @Christopher Lefelhocz, @Hila Qu, @WayneHaber,  @Steve Loyd, @lily, @kwiebers, @Davis Townsend, @s_awezec, @mkarampalas`


#### Salesforce

Message: We have identified a delay in the `Salesforce` data refresh and this problem potentially impacts any Sales related KPIs or SiSense dashboards. We are actively working on a resolution and will provide an update once the KPIs and SiSense dashboards have been brought up-to-date.

The `Salesforce` data in the warehouse and downstream models is accurate as of YYYY-MM-DD HH:MM UTC (HH:MM PST).

The DRI for this incident is `@username`.

The link to the Data Team Incident issue is <link>

`CC @Jake Bielecki, @Matt Benzaquen, @Jack Brennan, @Craig Mestel`

#### Zuora

Message: We have identified a delay in the `Zuora` data refresh and this problem potentially impacts any Financial KPIs or SiSense dashboards. We are actively working on a resolution and will provide an update once the KPIs and SiSense dashboards have been brought up-to-date.

The `Zuora` data in the warehouse and downstream models is accurate as of YYYY-MM-DD HH:MM UTC (HH:MM PST).

The DRI for this incident is `@username`.

The link to the Data Team Incident issue is <link>

`CC @Jake Bielecki, @Matt Benzaquen, @Jack Brennan, @Craig Mestel`

#### General

We have identified a delay in the `DATA SOURCE` data refresh. We are actively working on a resolution and will provide an update once data has been brought up-to-date.

The `DATA SOURCE` data in the warehouse and downstream models is accurate as of YYYY-MM-DD HH:MM UTC (HH:MM PST).

The DRI for this incident is `@username`.

The link to the Data Team Incident issue is <link>

</details>

## Finishing the Day

* [ ] At the end of your working day post EOD message to slack along with a link to this issue in the above mentioned slack channels so that it is clear for the next triager what time to check for issues from.
* [ ] Leave a comment giving context on open items and issues. If it's relevant to a specific issue, consider commenting in that issue and then linking to your comment.


/label ~"workflow::In dev" ~"Housekeeping" ~"Data Team" ~"Documentation" ~"Triage" ~"Priority::1-Ops"
