from pyspark.sql.streaming.listener import (
    QueryIdleEvent,
    QueryProgressEvent,
    QueryStartedEvent,
    QueryTerminatedEvent,
    StreamingQueryListener,
)

from utils.spark_logging import SparkLoggerManager


class BatchProgressListener(StreamingQueryListener):
    """마이크로 배치 진행 상황을 Log4j로 기록하는 스트리밍 리스너."""

    def onQueryStarted(self, event: QueryStartedEvent) -> None:
        logger = SparkLoggerManager().get_logger()
        if logger:
            logger.info(f"[Stream] Started: {event.name} (runId={event.runId})")

    def onQueryProgress(self, event: QueryProgressEvent) -> None:
        logger = SparkLoggerManager().get_logger()
        if not logger or not hasattr(event, "progress") or event.progress is None:
            return
        p = event.progress
        logger.info(
            f"[Stream] {p.name} batch={p.batchId} | rows={p.numInputRows} | in={p.inputRowsPerSecond:.1f}/s out={p.processedRowsPerSecond:.1f}/s"
        )

    def onQueryIdle(self, event: QueryIdleEvent) -> None:
        pass

    def onQueryTerminated(self, event: QueryTerminatedEvent) -> None:
        logger = SparkLoggerManager().get_logger()
        if not logger:
            return
        if event.exception:
            logger.error(f"[Stream] Terminated with error: {event.exception}")
        else:
            logger.info(f"[Stream] Terminated gracefully (id={event.id})")
