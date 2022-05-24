import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DBT_IMAGE,
    dbt_install_deps_nosha_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
)
from kube_secrets import (
    GIT_DATA_TESTS_PRIVATE_KEY,
    GIT_DATA_TESTS_CONFIG,
    SALT,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SALT_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}


# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "sla": timedelta(hours=12),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
    "dagrun_timeout": timedelta(hours=6),
}

# Create the DAG. Run daily at 04:05
dag = DAG("dbt_backups", default_args=default_args, schedule_interval="5 4 * * *")


def generate_task(task_name: str, table_list: list, included: bool = False) -> None:
    """
    @param task_name:
    @param table_list:
    @param included:
    @return:
    """

    # dbt run-operation for backups
    args = F"""'{{TABLE_LIST_BACKUP: {table_list}, INCLUDED: {included}}}'"""

    dbt_backups_cmd = f"""
        {dbt_install_deps_nosha_cmd} &&
        dbt run-operation backup_to_gcs --args {args} --profiles-dir profile
    """

    dbt_backups = KubernetesPodOperator(
        **gitlab_defaults,
        image=DBT_IMAGE,
        task_id=f"dbt-backups-{task_name}",
        name=f"dbt-backups-{task_name}",
        secrets=[
            GIT_DATA_TESTS_PRIVATE_KEY,
            GIT_DATA_TESTS_CONFIG,
            SALT,
            SALT_EMAIL,
            SALT_IP,
            SALT_NAME,
            SALT_PASSWORD,
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_USER,
            SNOWFLAKE_PASSWORD,
            SNOWFLAKE_TRANSFORM_ROLE,
            SNOWFLAKE_TRANSFORM_WAREHOUSE,
            SNOWFLAKE_TRANSFORM_SCHEMA,
        ],
        env_vars=pod_env_vars,
        arguments=[dbt_backups_cmd],
        dag=dag,
    )


TABLE_LIST_BACKUP_LARGE = ['FCT_MRR_SNAPSHOT', 'MART_ARR_SNAPSHOT', 'GITLAB_DOTCOM_PROJECT_STATISTICS_SNAPSHOTS', 'MART_WATERFALL_SNAPSHOT', 'MART_ARR_SNAPSHOT_20210609', 'GITLAB_DOTCOM_ISSUES_SNAPSHOTS', 'SFDC_ACCOUNT_SNAPSHOTS', 'DIM_SUBSCRIPTION_SNAPSHOT', 'MART_RETENTION_PARENT_ACCOUNT_SNAPSHOT', 'MART_CHARGE_SNAPSHOT', 'FCT_MRR_SNAPSHOT_20210531', 'GITLAB_DOTCOM_NAMESPACE_ROOT_STORAGE_STATISTICS_SNAPSHOTS', 'GITLAB_DOTCOM_PROJECTS_SNAPSHOTS', 'GITLAB_DOTCOM_NAMESPACES_SNAPSHOTS', 'GITLAB_DOTCOM_MEMBERS_SNAPSHOTS', 'MART_ARR_SNAPSHOT_20210531', 'GITLAB_DOTCOM_NAMESPACE_STATISTICS_SNAPSHOTS', 'SFDC_OPPORTUNITY_SNAPSHOTS','GITLAB_DOTCOM_GITLAB_SUBSCRIPTIONS_NAMESPACE_ID_SNAPSHOTS','MART_AVAILABLE_TO_RENEW_SNAPSHOT','GITLAB_DOTCOM_GITLAB_SUBSCRIPTIONS_SNAPSHOTS','ZUORA_REVENUE_REVENUE_CONTRACT_SCHEDULE_SNAPSHOTS','DIM_SUBSCRIPTION_SNAPSHOT_20210531','ZUORA_REVENUE_INVOICE_ACCOUNTING_SUMMARY_SNAPSHOTS', 'NETSUITE_TRANSACTION_LINES_SNAPSHOTS','ZUORA_RATEPLANCHARGE_SNAPSHOTS', 'ZUORA_REVENUE_SCHEDULE_ITEM_SNAPSHOTS']
TABLE_LIST_BACKUP_MID = ['GITLAB_DOTCOM_APPLICATION_SETTINGS_SNAPSHOTS']


backup_separation_spec = {}

backup_separation_spec['large'] = {'TABLE_LIST_BACKUP': TABLE_LIST_BACKUP_LARGE, 'INCLUDED': True}
backup_separation_spec['middle'] = {'TABLE_LIST_BACKUP': TABLE_LIST_BACKUP_MID, 'INCLUDED': True}
backup_separation_spec['rest'] = {'TABLE_LIST_BACKUP': TABLE_LIST_BACKUP_LARGE + TABLE_LIST_BACKUP_MID, 'INCLUDED': False}

for task_name, task_details in backup_separation_spec.items():

    generate_task(task_name=task_name,
                  table_list=task_details.get('TABLE_LIST_BACKUP'),
                  included=task_details.get('INCLUDED'))