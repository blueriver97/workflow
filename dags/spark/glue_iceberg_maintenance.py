from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DAG_ID = Path(__file__).name.removesuffix(".py")

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Lazy callback
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


def generate_env() -> dict:
    return {
        "SPARK_HOME": "{{ var.value.SPARK_HOME }}",
        "HADOOP_CONF_DIR": "{{ var.value.HADOOP_CONF_DIR }}",
        "PYSPARK_PYTHON": "{{ var.value.PYSPARK_PYTHON }}",
    }


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Iceberg data table maintenance (compaction + snapshot expire + orphan cleanup)",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    on_failure_callback=on_failure,
    on_success_callback=on_success,
    tags=["spark", "maintenance", "iceberg"],
) as dag:
    config_path = str(Path(__file__).parent.parent / "configs" / f"{DAG_ID}.yml")
    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    job = config["job"]
    retention_days = str(job["retention_days"])
    retain_last = str(job["retain_last"])
    target_file_size = str(job["target_file_size_bytes"])
    min_file_size = str(job["min_file_size_bytes"])
    schemas_csv = ",".join(job["schemas"])

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

    maintenance_task = SparkSubmitOperator(
        task_id="iceberg_maintenance",
        name=DAG_ID,
        conn_id="spark_default",
        application="/opt/airflow/src/iceberg_maintenance.py",
        py_files="/opt/airflow/src/utils.zip",
        application_args=[
            "--catalog",
            "{{ var.value.AWS_CATALOG }}",
            "--warehouse",
            "s3a://{{ var.value.AWS_S3_BUCKET }}{{ var.value.AWS_ICEBERG_PATH }}",
            "--schemas",
            schemas_csv,
            "--retention-days",
            retention_days,
            "--retain-last",
            retain_last,
            "--target-file-size",
            target_file_size,
            "--min-file-size",
            min_file_size,
        ],
        env_vars=generate_env(),
        conf=spark_conf,
        on_failure_callback=on_failure,
        on_success_callback=on_success,
    )
