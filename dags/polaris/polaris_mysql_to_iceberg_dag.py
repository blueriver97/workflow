import datetime

from airflow.models import DAG, Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DAG_ID = "polaris_mysql_to_iceberg"

# 환경 변수
VAULT_URL = Variable.get("VAULT_URL")
VAULT_USERNAME = Variable.get("VAULT_USERNAME")
VAULT_PASSWORD = Variable.get("VAULT_PASSWORD")
VAULT_SECRET_PATH = "secret/data/user/database/local-mysql"
HADOOP_CONF_DIR = Variable.get("HADOOP_CONF_DIR")
SPARK_HOME = Variable.get("SPARK_HOME")
PYSPARK_PYTHON = Variable.get("PYSPARK_PYTHON")
SPARK_DIST_CLASSPATH = Variable.get("SPARK_DIST_CLASSPATH")
ICEBERG_S3_ROOT_PATH = Variable.get("ICEBERG_S3_ROOT_PATH")

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
    "CATALOG": "polaris",
    "TABLES": "store.tb_lower,store.TB_UPPER,store.TB_COMPOSITE_KEY",
}

# Spark 설정
SPARK_CONF = {
    "spark.yarn.maxAppAttempts": "1",
    "spark.driver.cores": "1",
    "spark.driver.memory": "1G",
    "spark.executor.cores": "1",
    "spark.executor.memory": "1G",
    "spark.executor.instances": "1",
}

with DAG(
    dag_id=DAG_ID,
    description="Batch job to process DB tables and write to Iceberg using Spark",
    start_date=datetime.datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=[],
) as dag:
    submit_job = SparkSubmitOperator(
        conn_id="spark_default",
        task_id="submit_spark_job",
        spark_binary="/opt/spark/bin/spark-submit",
        name=DAG_ID,
        deploy_mode="cluster",
        application="/opt/airflow/src/polaris_mysql_to_iceberg.py",
        py_files="/opt/airflow/src/utils.zip",
        conf=SPARK_CONF,
        env_vars=ENV_VARS,
    )
