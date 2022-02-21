<!-- Subject format should be: YYYY-MM-DD | task name | Error line from log-->
<!-- example: 2020-05-15 | dbt-non-product-models-run | Database Error in model sheetload_manual_downgrade_dotcom_tracking -->

Airflow Task Link: <!-- link to airflow log with error -->

```
{longer error description text from log}
```

Downstream Airflow tasks or dbt models that were skipped: <!-- None -->
  <!-- list any downstream tasks that were skipped because of this error -->

## DE Triage Guidelines

* See the [handbook](https://about.gitlab.com/handbook/business-technology/data-team/how-we-work/triage/#triage-common-issues) for common triage issues.

<details>
<summary>dbt model failures</summary>
Should any model fail, you are welcome to investigate the issue end to end, however to ensure all of the errors are being addressed ensure the below is completed 

1. [ ] Check out the latest master branch and run the model locally to ensure the error is still valid. 
1. [ ] Check the git log for the problematic model, as well as any parent models. If there are any changes here which are obviously causing the problem, you can either: 
    1. [ ] If the problem is syntax and simple to solve (i.e. a missing comma) create an MR attached to the triage issue and correct the problem. Tag the last merger for review on the issue to confirm the change is correct and valid.
    1. [ ] If the problem is complicated or you are uncertain on how to solve it tag the CODEOWNER for the file as well as @gitlab-data/data-engineers to ensure everyone can see the issue.

##### For workspace models 

* [ ] As workspace models are only used for internal testing should there be any issues with this models tag the last merger on the issue.   
</details>

/label ~Triage ~"Team::Data Platform" ~Break-Fix ~"Priority::1-Ops" ~"workflow::1 - triage"
