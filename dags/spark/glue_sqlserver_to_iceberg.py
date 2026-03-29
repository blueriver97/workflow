from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

try:
    from airflow.sdk import task
except ImportError:
    from airflow.decorators import task
from datahub_airflow_plugin.entities import Dataset

DAG_ID = Path(__file__).name.removesuffix(".py")

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Lazy callback: DAG 파싱 시 SlackNotifier 인스턴스를 생성하지 않는다.
# ---------------------------------------------------------------------------
def _get_notifier():
    from alerts.slack_notifier import SlackNotifier

    return SlackNotifier(
        channel="#data-alerts",
        conn_id="slack_api",
        redis_host="redis",
        redis_port=6379,
        redis_db=0,
    )


def on_failure(context):
    _get_notifier().send_failure(context)


def on_success(context):
    _get_notifier().send_recovery(context)


@task(task_id=f"{DAG_ID}.get_mapped_configs")
def get_mapped_configs(config):
    """expand_kwargs에 직접 전달될 '리스트'만 반환 (ValueError 해결 핵심)"""
    tables = config["job"]["tables"]
    num_partition = str(config["job"]["num_partition"])

    result = []
    for table in tables:
        result.append(
            {
                "application_args": ["--table", table, "--num_partition", num_partition],
                "name": f"{table}",
            }
        )
    return result


def generate_lineage(config) -> tuple[list[Dataset], list[Dataset]]:
    """전체 테이블에 대한 inlets/outlets를 집계하여 반환"""
    catalog = Variable.get("AWS_CATALOG")
    tables = config["job"]["tables"]

    all_inlets = []
    all_outlets = []
    for table in tables:
        schema, _, table_name = table.split(".")
        all_inlets.append(Dataset(platform="sqlserver", name=f"{table}", env="PROD"))
        all_outlets.append(
            Dataset(platform="iceberg", name=f"{catalog}.{schema.lower()}_bronze.{table_name.lower()}", env="PROD")
        )
    return all_inlets, all_outlets


def generate_env() -> dict:
    env = {
        "SPARK_HOME": "{{ var.value.SPARK_HOME }}",
        "HADOOP_CONF_DIR": "{{ var.value.HADOOP_CONF_DIR }}",
        "PYSPARK_PYTHON": "{{ var.value.PYSPARK_PYTHON }}",
    }

    application_env = {
        "VAULT__URL": "{{ var.value.VAULT_URL }}",
        "VAULT__USERNAME": "{{ var.value.VAULT_USERNAME }}",
        "VAULT__PASSWORD": "{{ var.value.VAULT_PASSWORD }}",
        "VAULT__SECRET_PATH": "secret/data/user/database/local-sqlserver",
        "DATABASE__TYPE": "sqlserver",
        "AWS__PROFILE": "{{ var.value.AWS_PROFILE }}",
        "AWS__CATALOG": "{{ var.value.AWS_CATALOG }}",
        "AWS__S3_BUCKET": "{{ var.value.AWS_S3_BUCKET }}",
        "AWS__ICEBERG_PATH": "{{ var.value.AWS_ICEBERG_PATH }}",
    }
    env.update(application_env)

    return env


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="migrate data from sqlserver to iceberg",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    on_failure_callback=on_failure,
    on_success_callback=on_success,
) as dag:
    config_path = str(Path(__file__).parent.parent / "configs" / f"{DAG_ID}.yml")
    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    mapped_configs_list = get_mapped_configs(config)
    all_inlets, all_outlets = generate_lineage(config)

    env_vars = generate_env()

    # Variable.get() → Jinja 템플릿으로 대체하여 파싱 시 DB 쿼리 제거
    spark_conf = {
        "spark.yarn.maxAppAttempts": "1",
        "spark.driver.cores": "1",
        "spark.driver.memory": "1G",
        "spark.executor.cores": "1",
        "spark.executor.memory": "1G",
        "spark.executor.instances": "1",
        "spark.yarn.appMasterEnv.AWS_PROFILE": "{{ var.value.AWS_PROFILE }}",
        "spark.executorEnv.AWS_PROFILE": "{{ var.value.AWS_PROFILE }}",
        # OpenLineage Spark Listener
        "spark.extraListeners": "{{ var.value.OPENLINEAGE_SPARK_EXTRA_LISTENER }}",
        "spark.openlineage.transport.type": "http",
        "spark.openlineage.transport.url": "{{ var.value.OPENLINEAGE_URL }}",
        "spark.openlineage.transport.endpoint": "{{ var.value.OPENLINEAGE_ENDPOINT }}",
        "spark.openlineage.transport.auth.type": "api_key",
        "spark.openlineage.transport.auth.apiKey": "{{ var.value.OPENLINEAGE_API_KEY }}",
        "spark.openlineage.namespace": "prod",
        "spark.openlineage.appName": f"spark.prod.{DAG_ID}",
    }

    ingest_tables = SparkSubmitOperator.partial(
        task_id=f"{DAG_ID}.spark-submit",
        conn_id="spark_default",
        application="/opt/airflow/src/sqlserver_to_iceberg.py",
        py_files="/opt/airflow/src/utils.zip",
        map_index_template="{{task.name}}",
        env_vars=env_vars,
        conf=spark_conf,
        inlets=all_inlets,
        outlets=all_outlets,
        openlineage_inject_parent_job_info=True,
        openlineage_inject_transport_info=True,
    ).expand_kwargs(mapped_configs_list)

# if __name__ == "__main__":
#     dag.test()
