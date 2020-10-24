import os
import yaml
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from airflow_utils import (
    DATA_IMAGE,
    clone_repo_cmd,
    gitlab_defaults,
    slack_failed_task,
    DBT_IMAGE,
    dbt_install_deps_nosha_cmd,
    dbt_install_deps_and_seed_nosha_cmd,
    gitlab_pod_env_vars,
    xl_warehouse,
    xs_warehouse,
)
from kubernetes_helpers import get_affinity, get_toleration
from kube_secrets import (
    CUSTOMERS_DB_HOST,
    CUSTOMERS_DB_NAME,
    CUSTOMERS_DB_PASS,
    CUSTOMERS_DB_USER,
    GCP_SERVICE_CREDS,
    GIT_DATA_TESTS_CONFIG,
    GIT_DATA_TESTS_PRIVATE_KEY,
    GITLAB_COM_DB_HOST,
    GITLAB_COM_DB_NAME,
    GITLAB_COM_DB_PASS,
    GITLAB_COM_DB_USER,
    GITLAB_PROFILER_DB_HOST,
    GITLAB_PROFILER_DB_NAME,
    GITLAB_PROFILER_DB_PASS,
    GITLAB_PROFILER_DB_USER,
    PG_PORT,
    SALT,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SALT_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
)

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

dbt_secrets = [
    GIT_DATA_TESTS_CONFIG,
    GIT_DATA_TESTS_PRIVATE_KEY,
    SALT,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SALT_PASSWORD,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
]

validation_schedule_interval = "0 1 * * 0"
every_eighth_hour = "0 */8 * * *"
every_day_at_four = "0 4 */1 * *"

# Dictionary containing the configuration values for the various Postgres DBs
config_dict = {
    "customers": {
        "dag_name": "customers",
        "dbt_name": "customers_db",
        "env_vars": {"DAYS": "1"},
        "extract_schedule_interval": every_eighth_hour,
        "secrets": [
            CUSTOMERS_DB_USER,
            CUSTOMERS_DB_PASS,
            CUSTOMERS_DB_HOST,
            CUSTOMERS_DB_NAME,
        ],
        "start_date": datetime(2019, 5, 30),
        "sync_schedule_interval": "0 3 */1 * *",
        "task_name": "customers",
        "validation_schedule_interval": validation_schedule_interval,
    },
    "gitlab_com": {
        "dag_name": "gitlab_com",
        "dbt_name": "gitlab_dotcom",
        "env_vars": {"HOURS": "13"},
        "extract_schedule_interval": "0 */6 * * *",
        "secrets": [
            GITLAB_COM_DB_USER,
            GITLAB_COM_DB_PASS,
            GITLAB_COM_DB_HOST,
            GITLAB_COM_DB_NAME,
        ],
        "start_date": datetime(2019, 5, 30),
        "sync_schedule_interval": "0 2 */1 * *",
        "task_name": "gitlab-com",
        "validation_schedule_interval": validation_schedule_interval,
    },
    "gitlab_profiler": {
        "dag_name": "gitlab_profiler",
        "dbt_name": "none",
        "env_vars": {"DAYS": "3"},
        "extract_schedule_interval": "0 0 */1 * *",
        "secrets": [
            GITLAB_PROFILER_DB_USER,
            GITLAB_PROFILER_DB_PASS,
            GITLAB_PROFILER_DB_HOST,
            GITLAB_PROFILER_DB_NAME,
        ],
        "start_date": datetime(2019, 5, 30),
        "sync_schedule_interval": every_day_at_four,
        "task_name": "gitlab-profiler",
        "validation_schedule_interval": validation_schedule_interval,
    },
}


def generate_cmd(dag_name, operation):
    return f"""
        {clone_repo_cmd} &&
        cd analytics/extract/postgres_pipeline/postgres_pipeline/ &&
        python main.py tap ../manifests/{dag_name}_db_manifest.yaml {operation}
    """


def extract_manifest(file_path):
    with open(file_path, "r") as file:
        manifest_dict = yaml.load(file, Loader=yaml.FullLoader)
    return manifest_dict


def extract_table_list_from_manifest(manifest_contents):
    return manifest_contents["tables"].keys()


def dbt_tasks(dbt_name, dbt_task_identifier):

    freshness_cmd = f"""
        {dbt_install_deps_and_seed_nosha_cmd} &&
        dbt source snapshot-freshness --profiles-dir profile --target prod --select {dbt_name}; ret=$?;
        python ../../orchestration/upload_dbt_file_to_snowflake.py freshness; exit $ret
    """
    freshness = KubernetesPodOperator(
        **gitlab_defaults,
        image=DBT_IMAGE,
        task_id=f"{dbt_task_identifier}-source-freshness",
        name=f"{dbt_task_identifier}-source-freshness",
        secrets=standard_secrets + dbt_secrets,
        env_vars=gitlab_pod_env_vars,
        arguments=[freshness_cmd],
    )

    # Test raw source
    test_cmd = f"""
        {dbt_install_deps_nosha_cmd} &&
        dbt test --profiles-dir profile --target prod --models source:{dbt_name}; ret=$?;
        python ../../orchestration/upload_dbt_file_to_snowflake.py source_tests; exit $ret
    """
    test = KubernetesPodOperator(
        **gitlab_defaults,
        image=DBT_IMAGE,
        task_id=f"{dbt_task_identifier}-source-test",
        name=f"{dbt_task_identifier}-source-test",
        secrets=standard_secrets + dbt_secrets,
        env_vars=gitlab_pod_env_vars,
        arguments=[test_cmd],
    )

    # Snapshot source data
    snapshot_cmd = f"""
        {dbt_install_deps_nosha_cmd} &&
        dbt snapshot --profiles-dir profile --target prod --select source:{dbt_name} --vars {xl_warehouse}; ret=$?;
        python ../../orchestration/upload_dbt_file_to_snowflake.py snapshots; exit $ret
    """
    snapshot = KubernetesPodOperator(
        **gitlab_defaults,
        image=DBT_IMAGE,
        task_id=f"{dbt_task_identifier}-source-snapshot",
        name=f"{dbt_task_identifier}-source-snapshot",
        secrets=standard_secrets + dbt_secrets,
        env_vars=gitlab_pod_env_vars,
        arguments=[snapshot_cmd],
    )

    model_run_cmd = f"""
        {dbt_install_deps_nosha_cmd} &&
        dbt run --profiles-dir profile --target prod --models +sources.{dbt_name} --vars {xl_warehouse}; ret=$?;
        python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
    """
    model_run = KubernetesPodOperator(
        **gitlab_defaults,
        image=DBT_IMAGE,
        task_id=f"{dbt_task_identifier}-source-model-run",
        name=f"{dbt_task_identifier}-source-model-run",
        secrets=standard_secrets + dbt_secrets,
        env_vars=gitlab_pod_env_vars,
        arguments=[model_run_cmd],
    )

    # Test all source models
    model_test_cmd = f"""
        {dbt_install_deps_nosha_cmd} &&
        dbt test --profiles-dir profile --target prod --models +sources.{dbt_name}; ret=$?;
        python ../../orchestration/upload_dbt_file_to_snowflake.py test; exit $ret
    """
    model_test = KubernetesPodOperator(
        **gitlab_defaults,
        image=DBT_IMAGE,
        task_id=f"{dbt_task_identifier}-model-test",
        name=f"{dbt_task_identifier}-model-test",
        secrets=standard_secrets + dbt_secrets,
        env_vars=gitlab_pod_env_vars,
        arguments=[model_test_cmd],
    )

    return freshness, test, snapshot, model_run, model_test


# Loop through each config_dict and generate a DAG
for source_name, config in config_dict.items():

    # Extract DAG
    extract_dag_args = {
        "catchup": True,
        "depends_on_past": False,
        "on_failure_callback": slack_failed_task,
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "sla": timedelta(hours=8),
        "sla_miss_callback": slack_failed_task,
        "start_date": config["start_date"],
        "dagrun_timeout": timedelta(hours=6),
        "trigger_rule": "all_success",
    }
    extract_dag = DAG(
        f"{config['dag_name']}_db_extract",
        default_args=extract_dag_args,
        schedule_interval=config["extract_schedule_interval"],
    )

    with extract_dag:

        # dbt tasks
        dbt_name = f"{config['dbt_name']}"
        dbt_task_identifier = f"{config['task_name']}-dbt-incremental"

        freshness, test, snapshot, model_run, model_test = dbt_tasks(
            dbt_name, dbt_task_identifier
        )

        freshness >> test >> snapshot >> model_run >> model_test

        # Actual PGP extract
        file_path = f"analytics/extract/postgres_pipeline/manifests/{config['dag_name']}_db_manifest.yaml"
        manifest = extract_manifest(file_path)
        table_list = extract_table_list_from_manifest(manifest)
        for table in table_list:
            # tables without execution_date in the query won't be processed incrementally
            if "{EXECUTION_DATE}" not in manifest["tables"][table]["import_query"]:
                continue

            task_type = "db-incremental"
            task_identifier = (
                f"{config['task_name']}-{table.replace('_','-')}-{task_type}"
            )

            incremental_cmd = generate_cmd(
                config["dag_name"], f"--load_type incremental --load_only_table {table}"
            )

            incremental_extract = KubernetesPodOperator(
                **gitlab_defaults,
                image=DATA_IMAGE,
                task_id=f"{task_identifier}-pgp-extract",
                name=f"{task_identifier}-pgp-extract",
                pool=f"{config['task_name']}_pool",
                secrets=standard_secrets + config["secrets"],
                env_vars={
                    **gitlab_pod_env_vars,
                    **config["env_vars"],
                    "LAST_EXECUTION_DATE": "{{ execution_date }}",
                    "TASK_INSTANCE": "{{ task_instance_key_str }}",
                },
                affinity=get_affinity(False),
                tolerations=get_toleration(False),
                arguments=[incremental_cmd],
                do_xcom_push=True,
                xcom_push=True,
            )

            incremental_extract >> freshness

    globals()[f"{config['dag_name']}_db_extract"] = extract_dag

    # Sync DAG
    sync_dag_args = {
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

    sync_dag = DAG(
        f"{config['dag_name']}_db_sync",
        default_args=sync_dag_args,
        schedule_interval=config["sync_schedule_interval"],
        concurrency=1,
    )

    with sync_dag:
        # dbt Tasks
        dbt_name = f"{config['dbt_name']}"
        dbt_task_identifier = f"{config['task_name']}-dbt-sync"

        freshness, test, snapshot, model_run, model_test = dbt_tasks(
            dbt_name, dbt_task_identifier
        )

        freshness >> test >> snapshot >> model_run >> model_test

        # PGP Extract
        file_path = f"analytics/extract/postgres_pipeline/manifests/{config['dag_name']}_db_manifest.yaml"
        manifest = extract_manifest(file_path)
        table_list = extract_table_list_from_manifest(manifest)
        for table in table_list:
            task_type = "db-sync"
            if "{EXECUTION_DATE}" in manifest["tables"][table]["import_query"]:
                task_identifier = (
                    f"{config['task_name']}-{table.replace('_','-')}-{task_type}"
                )

                sync_cmd = generate_cmd(
                    config["dag_name"], f"--load_type sync --load_only_table {table}"
                )
                sync_extract = KubernetesPodOperator(
                    **gitlab_defaults,
                    image=DATA_IMAGE,
                    task_id=task_identifier,
                    name=task_identifier,
                    pool=f"{config['task_name']}_pool",
                    secrets=standard_secrets + config["secrets"],
                    env_vars={
                        **gitlab_pod_env_vars,
                        **config["env_vars"],
                        "TASK_INSTANCE": "{{ task_instance_key_str }}",
                    },
                    affinity=get_affinity(False),
                    tolerations=get_toleration(False),
                    arguments=[sync_cmd],
                    do_xcom_push=True,
                    xcom_push=True,
                )

                sync_extract >> freshness

            else:
                task_type = "db-scd"

                task_identifier = (
                    f"{config['task_name']}-{table.replace('_','-')}-{task_type}"
                )

                # SCD Task
                scd_cmd = generate_cmd(
                    config["dag_name"], f"--load_type scd --load_only_table {table}"
                )

                scd_extract = KubernetesPodOperator(
                    **gitlab_defaults,
                    image=DATA_IMAGE,
                    task_id=task_identifier,
                    name=task_identifier,
                    pool=f"{config['task_name']}_pool",
                    secrets=standard_secrets + config["secrets"],
                    env_vars={
                        **gitlab_pod_env_vars,
                        **config["env_vars"],
                        "TASK_INSTANCE": "{{ task_instance_key_str }}",
                    },
                    arguments=[scd_cmd],
                    affinity=get_affinity(True),
                    tolerations=get_toleration(True),
                    do_xcom_push=True,
                    xcom_push=True,
                )

                scd_extract >> freshness

    globals()[f"{config['dag_name']}_db_sync"] = sync_dag

    # Validation DAG
    validation_dag_args = {
        "catchup": True,
        "concurrency": 1,
        "depends_on_past": False,
        "on_failure_callback": slack_failed_task,
        "owner": "airflow",
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
        "start_date": datetime(2019, 1, 1),
    }
    validation_dag = DAG(
        f"{config['dag_name']}_db_validate",
        default_args=validation_dag_args,
        schedule_interval=config["validation_schedule_interval"],
    )

    with validation_dag:

        file_path = f"analytics/extract/postgres_pipeline/manifests/{config['dag_name']}_db_manifest.yaml"
        manifest = extract_manifest(file_path)
        table_list = extract_table_list_from_manifest(manifest)
        for table in table_list:
            # Validate Task
            validate_cmd = generate_cmd(
                config["dag_name"], f"--load_type validate --load_only_table {table}"
            )
            validate_ids = KubernetesPodOperator(
                **gitlab_defaults,
                image=DATA_IMAGE,
                task_id=f"{config['task_name']}-{table.replace('_', '-')}-db-validation",
                name=f"{config['task_name']}-{table.replace('_', '-')}-db-validation",
                pool=f"{config['task_name']}_pool",
                secrets=standard_secrets + config["secrets"],
                env_vars={
                    **gitlab_pod_env_vars,
                    **config["env_vars"],
                    "TASK_INSTANCE": "{{ task_instance_key_str }}",
                },
                affinity=get_affinity(False),
                tolerations=get_toleration(False),
                arguments=[validate_cmd],
                dag=validation_dag,
                do_xcom_push=True,
                xcom_push=True,
            )
    globals()[f"{config['dag_name']}_db_validation"] = validation_dag
