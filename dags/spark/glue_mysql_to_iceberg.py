from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def parse_yaml(config_file: Path):
    if config_file.exists():
        try:
            with open(config_file, encoding="utf-8") as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"Error parsing YAML file: {e}")
            raise
    else:
        print(f"Config file not found: {config_file}")
        raise FileNotFoundError(f"Config file not found: {config_file}")


def generate_spark_kwargs(config: dict):
    tables = config["job"]["tables"]
    num_partitions = config["job"]["num_partitions"]
    generated_config = []
    for table in tables:
        generated_config.append(
            {"application_args": ["--tables", table, "--num_partitions", str(num_partitions)], "name": table}
        )
    return generated_config


DAG_ID = Path(__file__).name.removesuffix(".py")
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    config_file = Path(__file__).parent.parent / "configs" / f"{DAG_ID}.yml"
    config = parse_yaml(config_file=config_file)
    mapped_kwargs = generate_spark_kwargs(config)
    aws_profile = config["aws"]["profile"]
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
        application="/opt/airflow/src/glue_mysql_to_iceberg.py",
        py_files="/opt/airflow/src/utils.zip",
        files=str(config_file),
        map_index_template="{{task.name}}",
        conf=spark_conf,
    ).expand_kwargs(mapped_kwargs)

# if __name__ == "__main__":
#     dag.test()
