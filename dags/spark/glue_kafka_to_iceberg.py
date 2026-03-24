from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow import DAG
from airflow.models import Variable
from datahub_airflow_plugin.entities import Dataset
from operators.custom_spark import StreamingSparkSubmitOperator

DAG_ID = Path(__file__).name.removesuffix(".py")

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}


# ---------------------------------------------------------------------------
# Lazy callback: SlackNotifier를 callback 호출 시점에만 생성하여
# DAG 파싱 단계의 Redis/Slack 연결 비용을 제거한다.
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
    """Task 실패 시: Slack 알림 전송"""
    _get_notifier().send_failure(context)


def on_retry(context):
    """Task 재시도 시: Slack 경고 + 이전 실행 잔여 리소스 정리 기록"""
    _get_notifier().send_retry(context)


def on_success(context):
    """Task 복구 시: 이전에 실패했던 경우에만 복구 알림 전송"""
    _get_notifier().send_recovery(context)


# ---------------------------------------------------------------------------
# Environment variables (Jinja 템플릿으로 Variable.get() 파싱 시점 호출 제거)
# ---------------------------------------------------------------------------
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
        "VAULT__SECRET_PATH": "secret/data/user/database/local-mysql",
        "DATABASE__TYPE": "mysql",
        "AWS__PROFILE": "{{ var.value.AWS_PROFILE }}",
        "AWS__CATALOG": "{{ var.value.AWS_CATALOG }}",
        "AWS__S3_BUCKET": "{{ var.value.AWS_S3_BUCKET }}",
        "AWS__ICEBERG_PATH": "{{ var.value.AWS_ICEBERG_PATH }}",
        "KAFKA__BOOTSTRAP_SERVERS": "{{ var.value.KAFKA_BOOTSTRAP_SERVERS }}",
        "KAFKA__SCHEMA_REGISTRY": "{{ var.value.KAFKA_SCHEMA_REGISTRY }}",
        "KAFKA__TOPIC_PREFIX": "topic_prefix",
        "KAFKA__METRIC_NAMESPACE": "metric_namespace",
        "KAFKA__MAX_OFFSETS_PER_TRIGGER": "1000000",
        "KAFKA__STARTING_OFFSETS": "earliest",
        # Signal file 경로를 Spark 앱에 전달 → 앱이 이 경로를 폴링하여 shutdown 감지
        "SIGNAL__BUCKET": "{{ var.value.AWS_S3_BUCKET }}",
        "SIGNAL__KEY": f"spark/signal/{DAG_ID}",
    }
    env.update(application_env)

    return env


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Kafka → Iceberg streaming ingestion with graceful shutdown",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    on_failure_callback=on_failure,
    on_success_callback=on_success,
    tags=["spark", "streaming", "kafka", "iceberg"],
) as dag:
    config_path = str(Path(__file__).parent.parent / "configs" / f"{DAG_ID}.yml")
    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    topics = config["job"]["topics"]
    catalog = Variable.get("AWS_CATALOG", "catalog")

    all_inlets = []
    all_outlets = []
    for topic in topics:
        _, schema, table_name = topic.split(".")
        all_inlets.append(Dataset(platform="kafka", name=topic, env="PROD"))
        all_outlets.append(
            Dataset(platform="iceberg", name=f"{catalog}.{schema.lower()}_bronze.{table_name.lower()}", env="PROD")
        )

    env_vars = generate_env()

    # spark_conf 내 Variable 참조도 Jinja 템플릿으로 대체
    spark_conf = {
        "spark.yarn.maxAppAttempts": "1",
        "spark.driver.cores": "1",
        "spark.driver.memory": "1G",
        "spark.executor.cores": "2",
        "spark.executor.memory": "2G",
        "spark.executor.instances": "2",
        "spark.yarn.appMasterEnv.AWS_PROFILE": "{{ var.value.AWS_PROFILE }}",
        "spark.executorEnv.AWS_PROFILE": "{{ var.value.AWS_PROFILE }}",
        # OpenLineage Spark Listener
        "spark.extraListeners": "{{ var.value.OPENLINEAGE_SPARK_EXTRA_LISTENER }}",
        "spark.openlineage.transport.type": "http",
        "spark.openlineage.transport.url": "{{ var.value.OPENLINEAGE_URL }}",
        "spark.openlineage.transport.endpoint": "{{ var.value.OPENLINEAGE_ENDPOINT }}",
        "spark.openlineage.transport.auth.type": "api_key",
        "spark.openlineage.transport.auth.apiKey": "{{ var.value.OPENLINEAGE_API_KEY }}",
        "spark.openlineage.appName": f"spark.prod.{DAG_ID}",
        "spark.openlineage.namespace": "prod",
    }

    ingest_task = StreamingSparkSubmitOperator(
        name=DAG_ID,
        task_id="submit_kafka_to_iceberg_job",
        conn_id="spark_default",
        application="/opt/airflow/src/kafka_to_iceberg.py",
        py_files="/opt/airflow/src/utils.zip",
        application_args=["--topics", str(",".join(topics))],
        env_vars=env_vars,
        conf=spark_conf,
        inlets=all_inlets,
        outlets=all_outlets,
        # Signal file 기반 graceful shutdown
        signal_bucket="{{ var.value.AWS_S3_BUCKET }}",
        signal_key=f"spark/signal/{DAG_ID}",
        # Task 레벨 콜백
        on_failure_callback=on_failure,
        on_success_callback=on_success,
        on_retry_callback=on_retry,
    )

if __name__ == "__main__":
    dag.test()
