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

<details>
<summary><b>Resolving Chronic dbt model/test failures</b></summary>
Should any model/test fail, ensure all of the errors are being addressed ensure the below is completed: 

1. [ ] Has the root cause of the failure been determined? If not, the triager should determine the root cause.
1. [ ] Is the test failure related to a row count failure AND there is no concerning problem with the table? If yes, consider using the LAG parameter in the row count test macro or use the row count test macro that leverages averages and standard deviations. If neither one of these options works, then move the test to blocked status and consider this for the new Data Observability tool.  
1. [ ] Is the dbt model/test failure related to a timeout issue? If yes, confirm that the model can build and test can run on its own by full refreshing the model in Airflow. Review the model to confirm it truly needs to have a full refresh. Note the run time in Airflow for the model to build. If the model needs to be refreshed, then move the issue to blocked status and consider this model as a candidate for DAG Flowsharding.   

</details>


/label ~Triage ~Break-Fix ~"Priority::1-Ops" ~"workflow::1 - triage" ~"Triage::Analytics" 
