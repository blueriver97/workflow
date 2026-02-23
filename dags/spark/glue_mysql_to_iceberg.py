from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import task

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


@task
def get_job_configs(config_file_path: str):
    """런타임에 YAML 파일을 읽고 Spark 실행 인자를 생성함"""
    import yaml  # 워커에서만 필요하므로 함수 내부 임포트

    path = Path(config_file_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    tables = config["job"]["tables"]
    num_partitions = config["job"]["num_partition"]

    print(f"Generated configs for {len(tables)} tables")

    # expand_kwargs가 기대하는 리스트 형식으로 반환
    return [
        {
            "application_args": ["--table", table, "--num_partition", str(num_partitions)],
            "name": table,
        }
        for table in tables
    ]


DAG_ID = Path(__file__).name.removesuffix(".py")
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="migrate data from mysql to iceberg",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    def generate_env():
        return {
            "SPARK_HOME": "{{ var.value.SPARK_HOME }}",
            "HADOOP_CONF_DIR": "{{ var.value.HADOOP_CONF_DIR }}",
            "PYSPARK_PYTHON": "{{ var.value.PYSPARK_PYTHON }}",
            "SPARK_DIST_CLASSPATH": "{{ var.value.SPARK_DIST_CLASSPATH }}",
        }

    def generate_application_env():
        return {
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

    config_path = str(Path(__file__).parent.parent / "configs" / f"{DAG_ID}.yml")
    mapped_configs = get_job_configs(config_path)

    env_vars = generate_env()
    env_vars.update(generate_application_env())

    aws_profile = Variable.get("AWS_PROFILE")
    spark_conf = {
        "spark.yarn.maxAppAttempts": "1",
        "spark.driver.cores": "1",
        "spark.driver.memory": "1G",
        "spark.executor.cores": "1",
        "spark.executor.memory": "1G",
        "spark.executor.instances": "1",
        "spark.yarn.appMasterEnv.AWS_PROFILE": aws_profile,
        "spark.executorEnv.AWS_PROFILE": aws_profile,
    }

    ingest_tables = SparkSubmitOperator.partial(
        task_id="spark-submit",
        conn_id="spark_default",
        application="/opt/airflow/src/mysql_to_iceberg.py",
        py_files="/opt/airflow/src/utils.zip",
        # files=str(application_env),
        map_index_template="{{task.name}}",
        env_vars=env_vars,
        conf=spark_conf,
    ).expand_kwargs(mapped_configs)

# if __name__ == "__main__":
#     dag.test()
