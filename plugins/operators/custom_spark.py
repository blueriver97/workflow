from __future__ import annotations

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


class LineageMappedSparkSubmitOperator(SparkSubmitOperator):
    """동적 lineage 매핑을 지원하는 SparkSubmitOperator."""

    template_fields = SparkSubmitOperator.template_fields + ("mapped_inlets", "mapped_outlets")

    def __init__(self, **kwargs):
        self.mapped_inlets = kwargs.pop("mapped_inlets", None)
        self.mapped_outlets = kwargs.pop("mapped_outlets", None)
        super().__init__(**kwargs)

    def execute(self, context):
        if self.mapped_inlets:
            self.inlets = self.mapped_inlets
        if self.mapped_outlets:
            self.outlets = self.mapped_outlets
        return super().execute(context)


class StreamingSparkSubmitOperator(SparkSubmitOperator):
    """
    Spark Structured Streaming용 SparkSubmitOperator.

    S3 signal file 기반 graceful shutdown을 지원한다.
    - execute: 잔여 signal 파일 정리 후 spark-submit 실행
    - on_kill: signal 파일 생성 → Spark 앱이 감지하여 graceful shutdown 수행

    Spark 드라이버는 signal_key 경로를 주기적으로 폴링하다가,
    파일이 감지되면 현재 micro-batch를 완료한 뒤 안전하게 종료한다.
    """

    template_fields = SparkSubmitOperator.template_fields + ("signal_bucket", "signal_key")

    def __init__(self, signal_bucket: str, signal_key: str, **kwargs):
        super().__init__(**kwargs)
        self.signal_bucket = signal_bucket
        self.signal_key = signal_key

    def _s3_client(self):
        import boto3

        return boto3.client("s3")

    def execute(self, context):
        # 이전 실행에서 남은 signal 파일 정리 (orphaned signal 방지)
        self._cleanup_signal()
        self.log.info("Signal cleaned. Starting spark-submit for streaming job.")
        return super().execute(context)

    def on_kill(self):
        # 1. Signal 파일 생성 → Spark 앱이 감지하여 graceful shutdown 시작
        self.log.info("Writing stop signal: s3://%s/%s", self.signal_bucket, self.signal_key)
        try:
            self._s3_client().put_object(Bucket=self.signal_bucket, Key=self.signal_key, Body=b"STOP")
        except Exception:
            self.log.warning("Failed to write stop signal", exc_info=True)

        # 2. 로컬 spark-submit 클라이언트 프로세스 종료
        #    YARN cluster mode에서는 드라이버가 클러스터에서 독립 실행되므로,
        #    signal 파일을 통해 드라이버가 자체적으로 shutdown을 수행한다.
        return super().on_kill()

    def _cleanup_signal(self):
        try:
            self._s3_client().delete_object(Bucket=self.signal_bucket, Key=self.signal_key)
        except Exception:
            self.log.debug("Signal cleanup skipped", exc_info=True)
