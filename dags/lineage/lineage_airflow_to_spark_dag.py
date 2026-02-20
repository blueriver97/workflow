from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# from airflow.sdk import Asset # noqa

# DAG ID
DAG_ID = "lineage_airflow_to_spark_dag"

# 1. 환경 변수 및 설정
VAULT_URL = Variable.get("VAULT_URL")
VAULT_USERNAME = Variable.get("VAULT_USERNAME")
VAULT_PASSWORD = Variable.get("VAULT_PASSWORD")
VAULT_SECRET_PATH = "secret/data/user/database/local-mysql"
HADOOP_CONF_DIR = Variable.get("HADOOP_CONF_DIR")
SPARK_HOME = Variable.get("SPARK_HOME")
PYSPARK_PYTHON = Variable.get("PYSPARK_PYTHON")
SPARK_DIST_CLASSPATH = Variable.get("SPARK_DIST_CLASSPATH")
ICEBERG_S3_ROOT_PATH = Variable.get("ICEBERG_S3_ROOT_PATH")
AWS_PROFILE = Variable.get("AWS_PROFILE")
# DataHub 설정
DATAHUB_GMS_URL = Variable.get("DATAHUB_GMS_URL")
DATAHUB_TOKEN = Variable.get("DATAHUB_TOKEN")

# 처리할 테이블 목록
TARGET_TABLES_STR = "store.tb_lower,store.TB_UPPER,store.TB_COMPOSITE_KEY"
CATALOG_NAME = "glue_catalog"

ENV_VARS = {
    "VAULT_URL": VAULT_URL,
    "VAULT_USERNAME": VAULT_USERNAME,
    "VAULT_PASSWORD": VAULT_PASSWORD,
    "VAULT_SECRET_PATH": VAULT_SECRET_PATH,
    "HADOOP_CONF_DIR": HADOOP_CONF_DIR,
    "SPARK_HOME": SPARK_HOME,
    "PYSPARK_PYTHON": PYSPARK_PYTHON,
    "SPARK_DIST_CLASSPATH": SPARK_DIST_CLASSPATH,
    "ICEBERG_S3_ROOT_PATH": ICEBERG_S3_ROOT_PATH,
    "CATALOG": CATALOG_NAME,
    "TABLES": TARGET_TABLES_STR,
}

# Spark 설정
SPARK_CONF = {
    "spark.yarn.maxAppAttempts": "1",
    "spark.driver.cores": "1",
    "spark.driver.memory": "1G",
    "spark.executor.cores": "1",
    "spark.executor.memory": "1G",
    "spark.executor.instances": "1",
    "spark.yarn.appMasterEnv.AWS_PROFILE": AWS_PROFILE,
    "spark.executorEnv.AWS_PROFILE": AWS_PROFILE,
    # Glue Catalog 설정
    f"spark.sql.catalog.{CATALOG_NAME}": "org.apache.iceberg.spark.SparkCatalog",
    f"spark.sql.catalog.{CATALOG_NAME}.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    f"spark.sql.catalog.{CATALOG_NAME}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    f"spark.sql.catalog.{CATALOG_NAME}.warehouse": ICEBERG_S3_ROOT_PATH,
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
    # OpenLineage Spark Listener 설정
    "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
    # OpenLineage Transport 설정
    "spark.openlineage.transport.type": "http",
    "spark.openlineage.transport.url": DATAHUB_GMS_URL,
    "spark.openlineage.transport.endpoint": "/openapi/openlineage/api/v1/lineage",
    "spark.openlineage.transport.auth.type": "api_key",
    "spark.openlineage.transport.auth.apiKey": DATAHUB_TOKEN,
    "spark.openlineage.appName": "spark.production_cluster.glue_mysql_to_iceberg",
    "spark.openlineage.namespace": "production_cluster",
}


def generate_outlets(tables_str, catalog):
    """
    입력된 테이블 문자열을 파싱하여 DataHub Dataset 객체 리스트를 반환합니다.
    Spark Job 로직(glue_mysql_to_iceberg.py)에 따라 대상 테이블 이름을 변환합니다.
    변환 규칙: {catalog}.{schema}_bronze.{table} (소문자 변환)
    """
    datasets = []
    if not tables_str:
        return datasets

    table_list = [t.strip() for t in tables_str.split(",")]
    for table_full_name in table_list:
        try:
            schema, table = table_full_name.split(".")
            # Spark Job의 로직 반영: 소문자 변환 및 _bronze 스키마 적용
            target_schema = f"{schema.lower()}_bronze"
            target_table = table.lower()

            # DataHub Dataset 이름은 전체 경로(Catalog 포함)를 사용하는 것이 안전함
            dataset_name = f"{catalog}.{target_schema}.{target_table}"
            dataset_uri = f"iceberg://{catalog}.{target_schema}.{target_table}"

            # Dataset(platform, name, env)
            datasets.append(Dataset(dataset_name, dataset_uri))
        except ValueError:
            continue

    return datasets


# Outlets 생성
outlets = generate_outlets(TARGET_TABLES_STR, CATALOG_NAME)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Airflow to Spark Job Lineage Example (MySQL to Iceberg)",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["datahub", "spark", "lineage", "iceberg"],
) as dag:
    submit_job = SparkSubmitOperator(
        conn_id="spark_default",
        task_id="run_spark_iceberg_openlineage",
        spark_binary="/opt/spark/bin/spark-submit",
        name=DAG_ID,
        deploy_mode="cluster",
        application="/opt/airflow/src/lineage/glue_mysql_to_iceberg_lineage.py",
        py_files="/opt/airflow/src/utils.zip",
        conf=SPARK_CONF,
        env_vars=ENV_VARS,
        outlets=outlets,
        openlineage_inject_parent_job_info=True,
        openlineage_inject_transport_info=False,
    )
