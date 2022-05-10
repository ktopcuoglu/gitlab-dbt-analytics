<!-- Subject format should be: YYYY-MM-DD | task name | Error line from log-->
<!-- example: 2020-05-15 | dbt-non-product-models-run | Database Error in model sheetload_manual_downgrade_dotcom_tracking -->

Airflow Task Link: <!-- link to airflow log with error -->

```
{longer error description text from log}
```

Downstream Airflow tasks or dbt models that were skipped: <!-- None -->
  <!-- list any downstream tasks that were skipped because of this error -->

## AE Triage Guidelines

<details>
<summary><b>dbt model/test failures</b></summary>
Should any model/test fail, ensure all of the errors are being addressed ensure the below is completed: 

1. [ ] Check the dbt audit columns in the model to see who created the model, who last updated the model, and when.
1. [ ] If the model was created within the last month, then assign the test or run failure issue to that developer. This will allow for a 1 month warranty period on the model where the creator of the model can resolve any test or run problems. 
1. [ ] For models outside of the 1 month warranty period, check out the latest master branch and run the model locally to ensure the error is still valid. 
1. [ ] For models outside of the 1 month warranty period, check the git log for the problematic model, as well as any parent models. If there are any changes here which are obviously causing the problem, you can either: 
    1. [ ] If the problem is syntax and simple to solve (i.e. a missing comma) create an MR attached to the triage issue and correct the problem. Tag the last merger for review on the issue to confirm the change is correct and valid.
    1. [ ] If the problem is complicated or you are uncertain on how to solve it tag the CODEOWNER for the file.

</details>


/label ~Triage ~Break-Fix ~"Priority::1-Ops" ~"workflow::1 - triage"
