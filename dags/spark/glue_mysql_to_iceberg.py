from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow import DAG
from airflow.models import Variable
from airflow.sdk import task
from alerts.slack_notifier import SlackNotifier
from datahub_airflow_plugin.entities import Dataset
from operators.custom_spark import CustomSparkSubmitOperator

DAG_ID = Path(__file__).name.removesuffix(".py")

# Initialize Notifier
slack_notifier = SlackNotifier(
    channel="#data-alerts", conn_id="slack_api", redis_host="redis", redis_port=6379, redis_db=0
)

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@task(task_id=f"{DAG_ID}.get_mapped_configs")
def get_mapped_configs(config):
    """expand_kwargs에 직접 전달될 '리스트'만 반환 (ValueError 해결 핵심)"""
    tables = config["job"]["tables"]
    num_partition = str(config["job"]["num_partition"])
    catalog = "catalog"

    result = []
    for table in tables:
        schema, table_name = table.split(".")
        inlet_urns = [Dataset(platform="mysql", name=f"{table}", env="PROD", platform_instance="data_pipeline")]
        outlet_urns = [
            Dataset(platform="iceberg", name=f"{catalog}.{schema.lower()}_bronze.{table_name.lower()}", env="PROD")
        ]

        result.append(
            {
                "application_args": ["--table", table, "--num_partition", num_partition],
                "name": f"{table}",
                "mapped_inlets": inlet_urns,  # 키 이름 변경
                "mapped_outlets": outlet_urns,  # 키 이름 변경
            }
        )
    return result


def generate_env() -> dict:
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
    description="migrate data from mysql to iceberg",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # DAG 레벨에 콜백 등록 (DAG 전체 실패 시 1회 호출)
    on_failure_callback=slack_notifier.send_failure,
    on_success_callback=slack_notifier.send_recovery,
) as dag:
    config_path = str(Path(__file__).parent.parent / "configs" / f"{DAG_ID}.yml")
    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    mapped_configs_list = get_mapped_configs(config)

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
        "spark.executor.cores": "1",
        "spark.executor.memory": "1G",
        "spark.executor.instances": "1",
        "spark.yarn.appMasterEnv.AWS_PROFILE": aws_profile,
        "spark.executorEnv.AWS_PROFILE": aws_profile,
        # OpenLineage Spark Listener 설정
        "spark.extraListeners": spark_extra_listener,
        # OpenLineage Transport 설정
        "spark.openlineage.transport.type": "http",
        "spark.openlineage.transport.url": datahub_gms_url,
        "spark.openlineage.transport.endpoint": datahub_openlineage_endpoint,
        "spark.openlineage.transport.auth.type": "api_key",
        "spark.openlineage.transport.auth.apiKey": datahub_token,
        "spark.openlineage.appName": f"spark.prod.{DAG_ID}",
        "spark.openlineage.namespace": "prod",
    }

    ingest_tables = CustomSparkSubmitOperator.partial(
        task_id=f"{DAG_ID}.spark-submit",
        conn_id="spark_default",
        application="/opt/airflow/src/mysql_to_iceberg.py",
        py_files="/opt/airflow/src/utils.zip",
        map_index_template="{{task.name}}",
        env_vars=env_vars,
        conf=spark_conf,
        openlineage_inject_parent_job_info=True,
        openlineage_inject_transport_info=True,
    ).expand_kwargs(mapped_configs_list)

# if __name__ == "__main__":
#     dag.test()
