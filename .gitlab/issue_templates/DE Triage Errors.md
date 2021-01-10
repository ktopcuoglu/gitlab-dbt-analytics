<!-- Subject format should be: YYYY-MM-DD | task name | Error line from log-->
<!-- example: 2020-05-15 | dbt-non-product-models-run | Database Error in model sheetload_manual_downgrade_dotcom_tracking -->

log: <!-- link to airflow log with error -->

```
{longer error description text from log}
```

Downstream Airflow tasks that were skipped: 
  <!-- list any downstream tasks that were skipped because of this error -->

Urgency:
- [ ] T1 - Needs resolution ASAP
- [ ] T2 - Resolution Required / Keep on Milestone
- [ ] T3 - Backlog
 
/label ~Triage ~Infrastructure ~Break-Fix ~"Priority::1-Ops" ~"workflow::1 - triage"
