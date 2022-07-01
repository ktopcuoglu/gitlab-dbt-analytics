{% docs mart_ci_runner_activity_monthly %}

Mart table containing quantitative data related to CI runner activity on GitLab.com.

These metrics are aggregated at a monthly grain per `dim_namespace_id`.

Additional identifier/key fields - `dim_ci_runner_id`, `dim_ci_pipeline_id`, `dim_ci_stage_id` have been included for Reporting purposes. 

Only activity since 2020-01-01 is being processed due to the high volume of the data.

{% enddocs %}

{% docs mart_product_usage_free_user_metrics_monthly %}
This table unions the sets of all Self-Managed and SaaS **free users**. The data from this table will be used for  Customer Product Insights by Sales team.

The grain of this table is namespace || uuid-hostname per month.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs mart_ci_runner_activity_daily %}
 
Mart table containing quantitative data related to CI runner activity on GitLab.com.
 
These metrics are aggregated at a daily grain per `dim_project_id`.

Additional identifier/key fields - `dim_ci_runner_id`, `dim_ci_pipeline_id`, `dim_ci_stage_id` have been included for Reporting purposes. 

Only activity since 2020-01-01 is being processed due to the high volume of the data.

{% enddocs %}

{% docs mart_user_request %}
 
Mart table that contains all user requests to the Gitlab product by the customers.
 
It unions `bdg_issue_user_request` and `bdg_epic_user_request` to have the product request that are contained both in the epics and issues in the `gitlab-org` group.
After that, it adds useful data around these issues and epics as well the crm_account and crm_opportunity links that are useful to prioritize those user requests.

{% enddocs %}