"""
## Info about DAG
This DAG is designed to perform adhoc full refresh of any required number of models.
Before running this DAG set dbt model for full refresh in Airflow Variable named DBT_MODEL_TO_FULL_REFRESH. 
The Warehouse to run the full refresh is set by default to XL size but incase of performance testing use DBT_WAREHOUSE_FOR_FULL_REFRESH variable to change the warehouse size.
"""
import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.models import Variable
from airflow_utils import (
    DBT_IMAGE,
    dbt_install_deps_and_seed_nosha_cmd,
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

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
}

# Create the DAG
dag = DAG(
    "dbt_full_refresh",
    default_args=default_args,
    schedule_interval=None,
    description="Adhoc DBT FULL Refresh",
)
dag.doc_md = __doc__

# read model for full-refresh from Airflow Variable
dbt_model_to_full_refresh = Variable.get(
    "DBT_MODEL_TO_FULL_REFRESH", default_var="test"
)

# Read the warehouse from the Airflow variable if passed else use XL warehouse for full refresh.
dbt_warehouse_for_full_refresh = Variable.get(
    "DBT_WAREHOUSE_FOR_FULL_REFRESH", default_var="TRANSFORMING_XL"
)

logging.info(
    f"Running full refresh for {dbt_model_to_full_refresh} on warehouse {dbt_warehouse_for_full_refresh}"
)

# dbt-full-refresh
dbt_full_refresh_cmd = f"""
    {dbt_install_deps_and_seed_nosha_cmd} &&
    export SNOWFLAKE_TRANSFORM_WAREHOUSE={dbt_warehouse_for_full_refresh} &&
    dbt run --profiles-dir profile --target prod --models {dbt_model_to_full_refresh} --full-refresh; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""
dbt_full_refresh = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-full-refresh",
    name="dbt-full-refresh",
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
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
    ],
    env_vars=pod_env_vars,
    arguments=[dbt_full_refresh_cmd],
    dag=dag,
)
