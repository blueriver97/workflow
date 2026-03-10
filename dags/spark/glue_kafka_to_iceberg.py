from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from alerts.slack_notifier import SlackNotifier
from datahub_airflow_plugin.entities import Dataset

DAG_ID = Path(__file__).name.removesuffix(".py")

slack_notifier = SlackNotifier(
    channel="#data-alerts", conn_id="slack_api", redis_host="redis", redis_port=6379, redis_db=0
)

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def get_lineage_urns(topics, catalog):
    """Generate DataHub Lineage Inlets and Outlets"""
    inlet_urns = []
    outlet_urns = []
    for topic in topics:
        parts = topic.split(".")
        if len(parts) >= 3:
            schema, table_name = parts[-2], parts[-1]
            inlet_urns.append(Dataset("kafka", topic, "PROD"))
            outlet_urns.append(Dataset("iceberg", f"{catalog}.{schema.lower()}_bronze.{table_name.lower()}", "PROD"))
    return inlet_urns, outlet_urns


def generate_env() -> dict:
    """Generate common and application-specific environment variables"""

    env = {
        "SPARK_HOME": "{{ var.value.SPARK_HOME }}",
        "HADOOP_CONF_DIR": "{{ var.value.HADOOP_CONF_DIR }}",
        "PYSPARK_PYTHON": "{{ var.value.PYSPARK_PYTHON }}",
        # "SPARK_DIST_CLASSPATH": "{{ var.value.SPARK_DIST_CLASSPATH }}",
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
    }
    env.update(application_env)

    return env


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="migrate data from kafka to iceberg using a single spark job",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    on_failure_callback=slack_notifier.send_failure,
    on_success_callback=slack_notifier.send_recovery,
) as dag:
    config_path = str(Path(__file__).parent.parent / "configs" / f"{DAG_ID}.yml")
    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    topics = config["job"]["topics"]
    catalog = Variable.get("AWS_CATALOG", "glue_catalog")

    inlets, outlets = get_lineage_urns(topics, catalog)
    env_vars = generate_env()

    aws_profile = Variable.get("AWS_PROFILE")
    datahub_gms_url = Variable.get("DATAHUB_GMS_URL")
    datahub_openlineage_endpoint = Variable.get("DATAHUB_OPENLINEAGE_ENDPOINT")
    datahub_token = Variable.get("DATAHUB_TOKEN")
    spark_extra_listener = Variable.get("SPARK_EXTRA_LISTENER")

    spark_conf = {
        "spark.yarn.maxAppAttempts": "1",
        "spark.driver.cores": "1",
        "spark.driver.memory": "1G",
        "spark.executor.cores": "2",  # Increased for potential multi-threading
        "spark.executor.memory": "2G",
        "spark.executor.instances": "2",
        "spark.yarn.appMasterEnv.AWS_PROFILE": aws_profile,
        "spark.executorEnv.AWS_PROFILE": aws_profile,
        "spark.extraListeners": spark_extra_listener,
        "spark.openlineage.transport.type": "http",
        "spark.openlineage.transport.url": datahub_gms_url,
        "spark.openlineage.transport.endpoint": datahub_openlineage_endpoint,
        "spark.openlineage.transport.auth.type": "api_key",
        "spark.openlineage.transport.auth.apiKey": datahub_token,
        "spark.openlineage.appName": f"spark.prod.{DAG_ID}",
        "spark.openlineage.namespace": "prod",
    }

    ingest_task = SparkSubmitOperator(
        name=DAG_ID,
        task_id="submit_kafka_to_iceberg_job",
        conn_id="spark_default",
        application="/opt/airflow/src/kafka_to_iceberg.py",
        py_files="/opt/airflow/src/utils.zip",
        application_args=["--topics", str(",".join(topics))],
        env_vars=env_vars,
        conf=spark_conf,
        inlets=inlets,
        outlets=outlets,
    )

if __name__ == "__main__":
    dag.test()
