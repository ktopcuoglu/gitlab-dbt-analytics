<!-- Subject format should be: YYYY-MM-DD | task name | Error line from log-->
<!-- example: 2020-05-15 | dbt-non-product-models-run | Database Error in model sheetload_manual_downgrade_dotcom_tracking -->
<!-- example: 2020-05-15 | monte-carlo-data | raw:snapshots - anomalies found in table netsuite_entity_snapshots | 35h since update -->

Notification Link: <!-- link to airflow log with error / Monte Carlo incident -->

```
{longer error description text from log}
```

Downstream Airflow tasks or dbt models that were skipped: <!-- None -->
  <!-- list any downstream tasks that were skipped because of this error -->


## DE Triage Guidelines

* See the [handbook](https://about.gitlab.com/handbook/business-technology/data-team/how-we-work/triage/#triage-common-issues) for common triage issues.

<details>
<summary><b>Source freshness failures</b></summary>

1. [ ] Confirm that there are no errors in our process which could be a cause. If there are no errors it is likely an external failure. 
2. [ ] Check the [source contact spreadsheet](https://docs.google.com/spreadsheets/d/1VKvqyn7wy6HqpWS9T3MdPnE6qbfH2kGPQDFg2qPcp6U/edit#gid=0) for details on who to contact to assist 
3. [ ] Add the label with the source to this issue.
4. [ ] In case the notification comes from Monte Carlo, investigate within the platform and if necessary, open an issue on GitLab. There is a possibility of quick-triaging Monte Carlo related incidents by reacting directly to the incident notification on Slack. You can mark the incident as `Fixed`, `Expected`, `False Positive`, `No Action Needed` or `Investigating`. 

 
</details>



/label ~Triage ~"Team::Data Platform" ~Break-Fix ~"Priority::1-Ops" ~"workflow::1 - triage"
