Please take notice of the new data source request [handbook page](https://about.gitlab.com/handbook/business-technology/data-team/how-we-work/new-data-source/) before submitting this new data source request. If you have any questions, please ask in the #data slack channel.
 
## Business Use Case (Please explain what this data will be used for):
 
 
## Request checklist
To support the new data source triage process, try to complete the below checklist. If there is anything not sure or unknown, you can skip that point.
 
* [ ] Prefix the issue name with 'New Data Source: ', e.g. 'New Data Source: NetSuite AP data'
* [ ] Review the [current data available in the EDW](https://about.gitlab.com/handbook/business-ops/data-team/platform/#extract-and-load) and confirm it concerns a new pipeline, or a change/extension of an existing pipeline:
  - [ ] Complete new pipeline
  - [ ] Change/extension of an existing pipeline: `     `
* [ ] Do any objects in this data source need to be snapshotted? If yes, please open separate issues to have the objects snapshotted.
* [ ] Does it contain MNPI data?
* [ ] Does it contain PII data?
* [ ] Severity in case of an incident
  - [ ] Critical - S1
  - [ ] High - S2
  - [ ] Medium - S3
  - [ ] Low - S4
* [ ] Who will be using this data, and where (dashboards, snowflake UI, etc.)?
  - `      `
 
 
* [ ] Please list and describe any data from this source that is sensitive (Classified as Red or Orange in our [Data Classification Policy](https://about.gitlab.com/handbook/engineering/security/data-classification-policy.html#data-classification-levels))?
 - _`{data fields, columns, or objects}`_
* [ ] Does this data have any agreed [SLO](https://about.gitlab.com/handbook/business-ops/data-team/platform/#slos-service-level-objectives-by-data-source) attached to it? If not:
   * [ ] How often does the data need to be refreshed?
 
## People matrix
| Role | Name | Gitlab Handle |
| ---- | ---- | ------------- |
| System owner | `Please provide` | `Please provide` | `Please provide` |
| Technical contact for data related questions | `Please provide` | `Please provide` |
| Technical contact for infrastructural related questions | `Please provide` | `Please provide` |
| Data access approval* | `Please provide` | `Please provide` |
| Business users who need to be informed in case of data outage | `Please provide` | `Please provide` |
 
* Data access approval will be involved in the Access Request process and need to give approval if a GitLab team member applies for raw data access.
 
## Integration Preparation
 
<!--
Sufficient access needs to be granted and verified before we can begin working on an automated extraction
--->
 
**Will there need to be access granted in order for a Data Engineer to extract this data? (example: New permissions or credentials to Salesforce in order to access the data)**
 - [ ] Yes
 - [ ] No
 - [ ] I don't know
 
**If Yes:**
- Prioritize giving access to a service account rather than any individual Data Engineer, if uncertain on which account to
use contact the Data Engineer assigned below for confirmation. 
- Where will access be required?
- Link to Access Request: <!-- This can be blank to start, will need to be added for prioritization -->
 
 
## Data Engineer tasks
 
**Triage**
* [ ] Determine the extraction solution via decision [diagram](https://about.gitlab.com/handbook/business-technology/data-team/how-we-work/new-data-source/#extraction-solution):
* [ ] Estimate the [issue points](https://about.gitlab.com/handbook/business-technology/data-team/how-we-work/#issue-pointing), based on the current information
 
**Admin**
 * [ ] Create issue for creation of extract process (not needed if using Stitch/FiveTran)
 * [ ] Create issue for dbt models
 * [ ] Create and link merge requests for updating relevant documentation
 
**Who should be responsible for making the data accessible and usable in the data warehouse?**
- [ ] Analyst: <!-- please tag them -->
- [ ] Analytics Engineer <!-- please tag them -->
- [ ] Data Engineer: <!-- please tag them -->
 
<!-- Do not edit below this line -->
 
/label ~"new data source" ~"workflow::1 - triage" ~"Team::Data Platform" ~"Priority::3-Other"
 
 
