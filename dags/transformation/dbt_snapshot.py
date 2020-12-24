import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python_operator import (
    BranchPythonOperator,
    ShortCircuitOperator,
    PythonOperator,
)
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow_utils import (
    DBT_IMAGE,
    dbt_install_deps_cmd,
    dbt_install_deps_nosha_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
    slack_snapshot_failed_task,
    dbt_install_deps_and_seed_cmd,
    clone_repo_cmd,
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
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}
task_secrets = [
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
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
]


pull_commit_hash = """export GIT_COMMIT="{{ var.value.dbt_hash }}" """

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_snapshot_failed_task,
    "owner": "airflow",
    "sla": timedelta(hours=12),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
    "dagrun_timeout": timedelta(hours=6),
}

# Create the DAG
# Runs 3x per day
dag = DAG("dbt_snapshots", default_args=default_args, schedule_interval="0 */8 * * *")

# dbt-snapshot for daily tag
dbt_snapshot_cmd = f"""
    {dbt_install_deps_nosha_cmd} &&
    dbt snapshot -s tag:daily --profiles-dir profile --exclude path:snapshots/zuora path:snapshots/sfdc; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py snapshots; exit $ret
"""

dbt_snapshot = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-snapshots",
    name="dbt-snapshots",
    secrets=task_secrets,
    env_vars=pod_env_vars,
    arguments=[dbt_snapshot_cmd],
    dag=dag,
)

dbt_commit_hash_setter = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-commit-hash-setter",
    name="dbt-commit-hash-setter",
    env_vars=pod_env_vars,
    arguments=[
        f"""{clone_repo_cmd} &&
            cd analytics/transform/snowflake-dbt/ &&
            mkdir -p /airflow/xcom/ &&
            echo "{{\\"commit_hash\\": \\"$(git rev-parse HEAD)\\"}}" >> /airflow/xcom/return.json
        """
    ],
    do_xcom_push=True,
    xcom_push=True,
    dag=dag,
)


def commit_hash_exporter(**context):
    Variable.set(
        "dbt_hash",
        context["ti"].xcom_pull(task_ids="dbt-commit-hash-setter", key="return_value")[
            "commit_hash"
        ],
    )


dbt_commit_hash_exporter = PythonOperator(
    task_id="dbt-commit-hash-exporter",
    provide_context=True,
    python_callable=commit_hash_exporter,
    dag=dag,
)

# run snapshots on large warehouse
dbt_snapshot_models_command = f"""
    {pull_commit_hash} &&
    {dbt_install_deps_and_seed_cmd} &&
    dbt run --profiles-dir profile --target prod --models +legacy.snapshots; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""

dbt_snapshot_models_run = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-run-model-snapshots",
    name="dbt-run-model-snapshots",
    trigger_rule="all_done",
    secrets=task_secrets,
    env_vars=pod_env_vars,
    arguments=[dbt_snapshot_models_command],
    dag=dag,
)

# dbt-test
dbt_test_snapshots_cmd = f"""
    {pull_commit_hash} &&
    {dbt_install_deps_cmd} &&
    dbt test --profiles-dir profile --target prod --models +legacy.snapshots; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py test; exit $ret
"""

dbt_test_snapshot_models = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-test-snapshots",
    name="dbt-test-snapshots",
    trigger_rule="all_done",
    secrets=task_secrets,
    env_vars=pod_env_vars,
    arguments=[dbt_test_snapshots_cmd],
    dag=dag,
)


def run_or_skip_dbt(current_seconds: int, dag_interval: int) -> bool:
    # Only run models and tests once per day
    if current_seconds < dag_interval:
        return True
    else:
        return False


SCHEDULE_INTERVAL_HOURS = 8
timestamp = datetime.now()
current_seconds = timestamp.hour * 3600
dag_interval = SCHEDULE_INTERVAL_HOURS * 3600

short_circuit = ShortCircuitOperator(
    task_id="short_circuit",
    python_callable=lambda: run_or_skip_dbt(current_seconds, dag_interval),
    dag=dag,
)

dbt_commit_hash_setter >> dbt_commit_hash_exporter >> dbt_snapshot >> short_circuit >> dbt_snapshot_models_run >> dbt_test_snapshot_models
