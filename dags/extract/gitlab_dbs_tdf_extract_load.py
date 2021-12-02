import os
import yaml
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python_operator import ShortCircuitOperator

from airflow_utils import (
    DATA_IMAGE,
    clone_repo_cmd,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars,
)
from kubernetes_helpers import get_affinity, get_toleration
from kube_secrets import (
    GCP_SERVICE_CREDS,
    GITLAB_COM_DB_HOST,
    GITLAB_COM_DB_NAME,
    GITLAB_COM_DB_PASS,
    GITLAB_COM_DB_USER,
    GITLAB_COM_PG_PORT,
    GITLAB_COM_CI_DB_NAME,
    GITLAB_COM_CI_DB_HOST,
    GITLAB_COM_CI_DB_PASS,
    GITLAB_COM_CI_DB_PORT,
    GITLAB_COM_CI_DB_USER,
    GITLAB_OPS_DB_USER,
    GITLAB_OPS_DB_PASS,
    GITLAB_OPS_DB_HOST,
    GITLAB_OPS_DB_NAME,
    PG_PORT,
    GCP_PROJECT,
    GCP_REGION,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)

""" This file is used to generate the TDF records count DAG and Task. It has been broken into 2 to keep it modular"""
# Load the env vars into a dict and set env vars
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
standard_secrets = [
    GCP_SERVICE_CREDS,
    PG_PORT,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_LOAD_ROLE,
]

# Create Dictionary entry for Data Quality check. This is seprate from above to avoid duplicate DAG creation.

config_dict_td_pgp = {
    "el_gitlab_com_trusted_data_extract_load": {
        "cloudsql_instance_name": None,
        "dag_name": "el_gitlab_com_trusted_data_extract_load",
        "dbt_name": "None",
        "env_vars": {},
        "extract_schedule_interval": "0 4 */1 * *",
        "secrets": [
            GITLAB_COM_DB_USER,
            GITLAB_COM_DB_PASS,
            GITLAB_COM_DB_HOST,
            GITLAB_COM_DB_NAME,
            GITLAB_COM_PG_PORT,
        ],
        "start_date": datetime(2021, 5, 21),
        "sync_schedule_interval": None,
        "task_name": "gitlab-com",
    },
    "el_gitlab_com_trusted_data_extract_load_ci": {
        "cloudsql_instance_name": None,
        "dag_name": "el_gitlab_com_trusted_data_extract_load_ci",
        "dbt_name": "None",
        "env_vars": {},
        "extract_schedule_interval": "0 2 */1 * *",
        "secrets": [
            GITLAB_COM_CI_DB_NAME,
            GITLAB_COM_CI_DB_HOST,
            GITLAB_COM_CI_DB_PASS,
            GITLAB_COM_CI_DB_PORT,
            GITLAB_COM_CI_DB_USER,
        ],
        "start_date": datetime(2021, 5, 21),
        "sync_schedule_interval": None,
        "task_name": "gitlab-com-ci",
    },
    "el_gitlab_ops_trusted_data_extract_load": {
        "cloudsql_instance_name": "ops-db-restore",
        "dag_name": "el_gitlab_ops_trusted_data_extract_load",
        "dbt_name": "None",
        "env_vars": {},
        "extract_schedule_interval": "0 4 */1 * *",
        "secrets": [
            GCP_PROJECT,
            GCP_REGION,
            GITLAB_OPS_DB_USER,
            GITLAB_OPS_DB_PASS,
            GITLAB_OPS_DB_HOST,
            GITLAB_OPS_DB_NAME,
        ],
        "start_date": datetime(2021, 11, 15),
        "sync_schedule_interval": None,
        "task_name": "gitlab-com-ops",
    },
}


def extract_manifest(file_path):
    with open(file_path, "r") as file:
        manifest_dict = yaml.load(file, Loader=yaml.FullLoader)
    return manifest_dict


def extract_table_list_from_manifest(manifest_contents):
    return manifest_contents["tables"].keys()


for source_name, config in config_dict_td_pgp.items():

    # Sync DAG
    data_quality_dag_args = {
        "catchup": False,
        "depends_on_past": False,
        "on_failure_callback": slack_failed_task,
        "owner": "airflow",
        "retries": 0,
        "retry_delay": timedelta(minutes=3),
        "start_date": config["start_date"],
        "dagrun_timeout": timedelta(hours=10),
        "trigger_rule": "all_success",
    }
    data_quality_dag = DAG(
        f"{config['dag_name']}",
        default_args=data_quality_dag_args,
        schedule_interval=config["extract_schedule_interval"],
        concurrency=2,
    )
    with data_quality_dag:

        # PGP Extract
        file_path = f"analytics/extract/postgres_pipeline/manifests_decomposed/{config['dag_name']}_db_manifest.yaml"
        manifest = extract_manifest(file_path)
        table_list = extract_table_list_from_manifest(manifest)
        for table in table_list:
            task_type = "trusted-data-postgres-extract-load"
            task_identifier = (
                f"{config['task_name']}-{table.replace('_','-')}-{task_type}"
            )
            if config["cloudsql_instance_name"] is None:
                # td-pgp-extract Task
                td_pgp_extract_cmd = f"""
                {clone_repo_cmd} &&
                cd analytics/extract/postgres_pipeline/postgres_pipeline/ &&
                python main.py tap ../manifests_decomposed/{config["dag_name"]}_db_manifest.yaml --load_type trusted_data --load_only_table {table}
            """
            else:
                td_pgp_extract_cmd = f"""
                {clone_repo_cmd} &&
                cd analytics/orchestration &&
                python ci_helpers.py use_proxy --instance_name {config["cloudsql_instance_name"]} --command " \
                    python ../extract/postgres_pipeline/postgres_pipeline/main.py tap \
                    ../extract/postgres_pipeline/manifests_decomposed/{config["dag_name"]}_db_manifest.yaml --load_type trusted_data --load_only_table {table}"
            """
            td_pgp_extract = KubernetesPodOperator(
                **gitlab_defaults,
                image=DATA_IMAGE,
                task_id=task_identifier,
                name=task_identifier,
                # pool=f"{config['task_name']}_pool",
                pool="default_pool",
                secrets=standard_secrets + config["secrets"],
                env_vars={
                    **gitlab_pod_env_vars,
                    **config["env_vars"],
                    "TASK_INSTANCE": "{{ task_instance_key_str }}",
                    "task_id": task_identifier,
                },
                arguments=[td_pgp_extract_cmd],
                affinity=get_affinity(True),
                tolerations=get_toleration(True),
                do_xcom_push=True,
            )
            td_pgp_extract

    globals()[f"{config['dag_name']}_td_pgp_extract"] = data_quality_dag
