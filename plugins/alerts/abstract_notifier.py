from abc import abstractmethod
from typing import Any

from airflow.utils.log.logging_mixin import LoggingMixin

# Airflow 2.x: airflow.sdk 모듈 없음 → dict로 대체
try:
    from airflow.sdk import Context
except ImportError:
    Context = dict[str, Any]


class Notifier(LoggingMixin):
    """
    알림을 전송하는 유틸리티 클래스입니다.
    """

    def __init__(self, conn_id: str = ""):
        self.conn_id = conn_id

    @abstractmethod
    def send_failure(self, context: Context, message: str = "") -> None:
        pass

    @abstractmethod
    def send_retry(self, context: Context, message: str = "") -> None:
        pass

    @abstractmethod
    def send_recovery(self, context: Context, message: str = "") -> None:
        pass

    @abstractmethod
    def _get_event_key(self, dag_id: str, task_id: str, run_id: str) -> str:
        pass

    @abstractmethod
    def _get_or_create_parent(self, dag_id: str, task_id: str, run_id: str) -> str:
        pass

    @abstractmethod
    def _store_ts(self, dag_id: str, task_id: str, run_id: str, ts: str) -> None:
        pass
