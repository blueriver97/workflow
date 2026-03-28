from pyspark.sql import SparkSession


def build_signal_path(bucket: str, dag_id: str) -> str:
    """DAG ID로부터 S3 시그널 파일 경로를 생성한다."""
    return f"s3a://{bucket}/spark/signal/{dag_id}"  # DAG의 SIGNAL__KEY 규칙과 일치


def check_stop_signal(spark: SparkSession, signal_path: str) -> bool:
    """S3 시그널 파일 존재 여부를 확인하여 중단 신호를 감지한다."""
    try:
        jvm = spark._jvm
        path = jvm.org.apache.hadoop.fs.Path(signal_path)
        fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
        return fs.exists(path)
    except Exception:
        return False
