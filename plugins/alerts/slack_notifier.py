from __future__ import annotations

import json
import time

import redis
from airflow.providers.slack.hooks.slack import SlackHook
from airflow.utils.context import Context
from alerts.abstract_notifier import Notifier


class SlackNotifier(Notifier):
    """
    Airflow 태스크 이벤트에 대해 Slack Thread 기반 알림을 전송하는 유틸리티 클래스.

    - Redis를 사용하여 멱등성을 보장한다.
    - 최초 실패 시 부모 메시지를 생성한다.
    - 재시도/복구는 동일 Thread에 연결된다.
    - 월 단위 집계 카운트를 Redis에 저장한다.
    """

    THREAD_TTL_SECONDS = 60 * 60 * 24 * 35  # 35 days
    LOCK_TTL_SECONDS = 60  # 1 minute lock

    def __init__(
        self,
        channel: str,
        conn_id: str = "slack_api",
        redis_host: str = "redis",
        redis_port: int = 6379,
        redis_db: int = 0,
    ):
        super().__init__(conn_id)
        self.channel = channel
        self.redis = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        self.slack_hook = SlackHook(slack_conn_id=self.conn_id)

    # =========================================================
    # Public API
    # =========================================================

    def send_failure(self, context: Context, message: str = "") -> None:
        """
        태스크 실패 시 호출된다.

        - 부모 메시지가 없으면 생성
        - 이미 실패 상태면 중복 전송하지 않음
        - 월간 실패 카운트 증가
        """
        ti = context.get("task_instance")
        dag_id = ti.dag_id
        task_id = ti.task_id
        run_id = ti.run_id

        parent_ts = self._get_or_create_parent(dag_id, task_id, run_id)

        state = self._get_state(dag_id, task_id, run_id)
        if state == "FAILED":
            self.log.info("Duplicate failure detected. Skipping message.")
            return

        msg = message or f"Task Failed: {task_id}"
        self._post_thread_message(parent_ts, f":x: FAILED\n{msg}")
        self._set_state(dag_id, task_id, run_id, "FAILED")
        self._increment_monthly_counter(dag_id, task_id)

    def send_retry(self, context: Context, message: str = "") -> None:
        """
        태스크 재시도 시 호출된다.

        - 부모 메시지가 없으면 생성
        - Thread에 RETRY 메시지 추가
        """
        ti = context.get("task_instance")
        dag_id = ti.dag_id
        task_id = ti.task_id
        run_id = ti.run_id

        parent_ts = self._get_or_create_parent(dag_id, task_id, run_id)
        msg = message or f"Task Retrying: {task_id}"
        self._post_thread_message(parent_ts, f":warning: RETRY\n{msg}")
        self._set_state(dag_id, task_id, run_id, "RETRY")

    def send_recovery(self, context: Context, message: str = "") -> None:
        """
        태스크 복구(성공) 시 호출된다.

        - 부모 Thread에 SUCCESS 메시지 추가
        - 상태를 RECOVERED로 변경
        """
        ti = context.get("task_instance")
        dag_id = ti.dag_id
        task_id = ti.task_id
        run_id = ti.run_id

        # Only send recovery if it was previously failed/retried (try_number > 1)
        if ti.try_number <= 1:
            return

        parent_ts = self._get_or_create_parent(dag_id, task_id, run_id)
        msg = message or f"Task Recovered: {task_id}"
        self._post_thread_message(parent_ts, f":white_check_mark: RECOVERED\n{msg}")
        self._set_state(dag_id, task_id, run_id, "RECOVERED")

    # =========================================================
    # Internal - Key Management
    # =========================================================

    def _get_event_key(self, dag_id: str, task_id: str, run_id: str) -> str:
        """
        run 단위 Thread 관리를 위한 Redis key 생성.
        """
        return f"airflow:thread:{dag_id}:{task_id}:{run_id}"

    def _get_monthly_key(self) -> str:
        """
        월 단위 집계를 위한 Redis key 생성.
        """
        ym = time.strftime("%Y%m")
        return f"airflow:alert:{ym}"

    # =========================================================
    # Internal - Parent Message Logic
    # =========================================================

    def _get_or_create_parent(self, dag_id: str, task_id: str, run_id: str) -> str:
        """
        부모 Slack 메시지를 조회하거나 없으면 생성한다.
        SET NX EX를 이용하여 멱등성을 보장한다.
        """
        event_key = self._get_event_key(dag_id, task_id, run_id)
        lock_key = f"{event_key}:lock"

        existing = self.redis.get(event_key)
        if existing:
            data = json.loads(existing)
            self.log.info("Existing parent message found.")
            return data["ts"]

        # Try acquiring lock
        locked = self.redis.set(
            lock_key,
            "1",
            nx=True,
            ex=self.LOCK_TTL_SECONDS,
        )

        if not locked:
            self.log.info("Lock not acquired. Waiting for parent creation.")
            time.sleep(1)
            existing = self.redis.get(event_key)
            if existing:
                data = json.loads(existing)
                return data["ts"]
            raise RuntimeError("Failed to acquire parent message lock.")

        self.log.info("Creating new parent Slack message.")

        # DAG ID를 포함하여 메시지 생성
        response = self.slack_hook.call(
            "chat.postMessage",
            json={
                "channel": self.channel,
                "text": f"🚨 Airflow Task Alert\nDAG: {dag_id}\nTask: {task_id}\nRun: {run_id}",
            },
        )

        parent_ts = response["ts"]
        self._store_ts(dag_id, task_id, run_id, parent_ts)
        self.redis.delete(lock_key)

        return parent_ts

    def _store_ts(self, dag_id: str, task_id: str, run_id: str, ts: str) -> None:
        """
        부모 메시지 ts 및 상태 정보를 Redis에 저장한다.
        """
        event_key = self._get_event_key(dag_id, task_id, run_id)
        payload = {
            "ts": ts,
            "state": "INIT",
            "created_at": int(time.time()),
        }

        self.redis.set(
            event_key,
            json.dumps(payload),
            ex=self.THREAD_TTL_SECONDS,
        )

        self.log.info("Parent message stored in Redis.")

    # =========================================================
    # Internal - State Handling
    # =========================================================

    def _get_state(self, dag_id: str, task_id: str, run_id: str) -> str | None:
        """
        현재 이벤트 상태를 조회한다.
        """
        event_key = self._get_event_key(dag_id, task_id, run_id)
        data = self.redis.get(event_key)
        if not data:
            return None
        return json.loads(data).get("state")

    def _set_state(self, dag_id: str, task_id: str, run_id: str, state: str) -> None:
        """
        이벤트 상태를 갱신한다.
        """
        event_key = self._get_event_key(dag_id, task_id, run_id)
        data = self.redis.get(event_key)
        if not data:
            return

        payload = json.loads(data)
        payload["state"] = state

        self.redis.set(
            event_key,
            json.dumps(payload),
            ex=self.THREAD_TTL_SECONDS,
        )

        self.log.info("State updated to %s", state)

    # =========================================================
    # Internal - Slack Thread Posting
    # =========================================================

    def _post_thread_message(self, parent_ts: str, text: str) -> None:
        """
        Slack Thread에 메시지를 전송한다.
        """
        self.slack_hook.call(
            "chat.postMessage",
            json={
                "channel": self.channel,
                "text": text,
                "thread_ts": parent_ts,
            },
        )
        self.log.info("Thread message posted.")

    # =========================================================
    # Internal - Monthly Aggregation
    # =========================================================

    def _increment_monthly_counter(self, dag_id: str, task_id: str) -> None:
        """
        월 단위 실패 카운트를 증가시킨다.
        """
        monthly_key = self._get_monthly_key()
        field = f"{dag_id}:{task_id}"

        self.redis.hincrby(monthly_key, field, 1)
        self.redis.expire(monthly_key, self.THREAD_TTL_SECONDS)

        self.log.info("Monthly failure counter incremented.")
