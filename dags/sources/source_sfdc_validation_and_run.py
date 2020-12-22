import os
from datetime import datetime

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
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}
pod_secrets = [
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

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "params": {"slack_channel_override": "#dbt-runs"},
    "owner": "airflow",
    "retries": 0,
    "trigger_rule": "all_success",
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
}

data_source = "sfdc"

# Create the DAG
# Run twice per day, 10 minutes after every 12th hour
dag = DAG(
    f"source_{data_source}_validation_and_run",
    default_args=default_args,
    schedule_interval="10 */12 * * *",
    description=f"This DAG tests the raw data for {data_source}, runs any snapshots, runs the source models, and tests the source models.",
)

# Raw source Freshness
freshness_cmd = f"""
    {dbt_install_deps_nosha_cmd} &&
    dbt source snapshot-freshness --profiles-dir profile --target prod --select {data_source}; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py freshness; exit $ret
"""
freshness = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id=f"{data_source}-source-freshness",
    name=f"{data_source}-source-freshness",
    secrets=pod_secrets,
    env_vars=pod_env_vars,
    arguments=[freshness_cmd],
    dag=dag,
)

# Test raw source
test_cmd = f"""
    {dbt_install_deps_nosha_cmd} &&
    dbt test --profiles-dir profile --target prod --models source:{data_source}; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py source_tests; exit $ret
"""
test = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id=f"{data_source}-source-test",
    name=f"{data_source}-source-test",
    secrets=pod_secrets,
    env_vars=pod_env_vars,
    arguments=[test_cmd],
    dag=dag,
)

# Snapshot source data
snapshot_cmd = f"""
    {dbt_install_deps_nosha_cmd} &&
    dbt snapshot --profiles-dir profile --target prod --select path:snapshots/{data_source}; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py snapshots; exit $ret
"""
snapshot = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id=f"{data_source}-source-snapshot",
    name=f"{data_source}-source-snapshot",
    secrets=pod_secrets,
    env_vars=pod_env_vars,
    arguments=[snapshot_cmd],
    dag=dag,
)

# Run source models
model_run_cmd = f"""
    {dbt_install_deps_nosha_cmd} &&
    dbt run --profiles-dir profile --target prod --models +sources.{data_source}; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""
model_run = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id=f"{data_source}-source-model-run",
    name=f"{data_source}-source-model-run",
    secrets=pod_secrets,
    env_vars=pod_env_vars,
    arguments=[model_run_cmd],
    dag=dag,
)

# Test all source models
model_test_cmd = f"""
    {dbt_install_deps_nosha_cmd} &&
    dbt test --profiles-dir profile --target prod --models +sources.{data_source}; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py test; exit $ret
"""
model_test = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id=f"{data_source}-model-test",
    name=f"{data_source}-model-test",
    secrets=pod_secrets,
    env_vars=pod_env_vars,
    arguments=[model_test_cmd],
    dag=dag,
)

freshness >> test >> snapshot >> model_run >> model_test
