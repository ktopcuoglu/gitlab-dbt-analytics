### Dashboard Review Process

Dashboard Name: 
Link to Dashboard: `WIP:` should be in the title and it should be in the WIP topic
Link to Handbook Page(s):
Issue Link:
Dashboard Owner/Editor:

#### Dashboard Creator:
<details>
<summary><i>Dashboard Creator complete this section/i></summary>
- [ ] What is the goal of this dashboard?
- [ ] Who is the primary audience?
- [ ] What's the primary consumption for this dashboard (i.e. handbook page, key meeting,etc)
- [ ] Does the dashboard provide the data requested?
- [ ] How did you validate data on the dashboard? (i.e. compared against source, test cases)
- [ ] Does the data align with the existing single source of truth and across applicable reporting in Periscope and/or Google Sheets? I.e. the data source is sourced from PROD.COMMON.*, AND REFERENCES FCT, DIM, or MART tables
- [ ] SQL formatted using [GitLab Style Guide](https://about.gitlab.com/handbook/business-ops/data-team/platform/sql-style-guide/)
- [ ] Is the underlying model built entirely on the Enterprise Dimensional Model? 
- [ ] Does this dashboard adhere to the [SiSense Style Guide](https://about.gitlab.com/handbook/business-ops/data-team/platform/sisense-style-guide/)
- [ ] Does an updated timestamp exist in the following format: 'Data As Of:' date/timestamp representing the most recent dbt run completion time for the supporting FCT
- [ ] Is there a indicator of which version this dashboard is. 
- [ ] Python / R reviewed for content, formatting, and necessity, if relevant (Did you write Python or R?)
- [ ] Filters, if relevant (Did you use filters?)
- [ ] Current month (in-progress) numbers and historical numbers are in separate charts  (If today is July 15, July should not be in your charts.)- Here's how to do it.
- [ ] Are there any drill downs?
- [ ] Overview/KPI/Top Level Performance Indicators are cross-linked to the handbook
- [ ] Topics (Periscope-speak for Categories) added
- [ ] Permissions reviewed
- [ ] Visualization Titles changed to Autofit, if relevant
- [ ] Axes labeled, if relevant
- [ ] Numbers (Currencies, Percents, Decimal Places, etc) cleaned, if relevant
- [ ] If using a date filter, set an appropriate length. Most common is 365 days. - Here's how to do it
- [ ] Chart description for each chart, linking to the handbook definitions where possible
- [ ] Legend for each of the charts is clear, and at bottom of chart
- [ ] Text Tile for "What am I looking at?" and more detailed information, leveraging hyperlinks instead of URLs
- [ ] Tooltips are used where appropriate and show relevant values
- [ ] Assign to reviewer on the data team. Use `@gitlab-data` on the dashboard if you don't know who to assign to
</details>

## Dashboard Reviewer:
<details>
<summary><i>Dashboard Reviewer complete this section/i></summary>
- [ ] Validate the source of the dashboard's underlying queries.
    * Are we we using tables sourced from PROD.COMMON when possible
    * Are we using the correct FCT, DIM, or MART tables
- [ ] Validate filters are working correctly
- [ ] Validate drill downs are working correctly
- [ ] Adheres to Sisense Style Guide
- [ ] Adheres to GitLab Style Guide
- [ ] Includes a `Data as of:` date/timestamp representing the most recent dbt run completion time for the supporting FCT
- [ ] Includes Version
- [ ] Text tile for "What am I looking at?"
- [ ] Tooltips used show appropriate values
- [ ] Axes labelled correctly
- [ ] Added to topics 
- [ ] Associated handbook page
</details>

## After Review Process is Completed and reccomendations from reviewer accounted for:
- [ ] Dashboard Creator: Add prefix `TD` to Level 2 Dashboards to indicate this is a trusted data solution
- [ ] Dashboard Creator: Add to appropriate topics as well as Trusted Data Framework Topic

/label ~Reporting ~Periscope / Sisense ~Dashboard Review

