from abc import abstractmethod

from airflow.sdk import Context
from airflow.utils.log.logging_mixin import LoggingMixin


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
    def _get_event_key(self) -> None:
        pass

    @abstractmethod
    def _get_or_create_parent(self) -> None:
        pass

    @abstractmethod
    def _store_ts(self) -> None:
        pass
