import requests
from airflow.models import Variable
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.utils.context import Context
from airflow.utils.log.logging_mixin import LoggingMixin


class SlackNotifier(LoggingMixin):
    """
    Slack 알림을 전송하는 유틸리티 클래스입니다.
    기본적인 알림 전송과 Spark(YARN) 애플리케이션 로그 링크를 포함한 실패 알림을 지원합니다.
    """

    def __init__(self, slack_conn_id: str = "slack_default"):
        self.slack_conn_id = slack_conn_id

    def _get_yarn_application_url(self, app_name: str) -> str | None:
        """
        YARN ResourceManager API를 호출하여 애플리케이션의 Tracking URL을 조회합니다.
        """
        yarn_api_url = Variable.get("YARN_API_URL", default_var=None)
        if not yarn_api_url:
            self.log.warning("Variable 'YARN_API_URL'이 설정되지 않았습니다.")
            return None

        try:
            # YARN Apps API 호출 (상태가 FAILED, KILLED인 앱도 조회될 수 있도록 파라미터 조정 가능)
            # 여기서는 실행 중이거나 완료된 모든 앱 중에서 이름이 일치하는 것을 찾습니다.
            response = requests.get(
                f"{yarn_api_url}/ws/v1/cluster/apps", params={"state": "FAILED,KILLED,FINISHED"}, timeout=5
            )
            response.raise_for_status()

            data = response.json()
            apps = data.get("apps", {}).get("app", [])

            # 가장 최근에 실행된 앱을 찾기 위해 시작 시간 역순 정렬 (선택 사항)
            # apps.sort(key=lambda x: x.get("startedTime", 0), reverse=True)

            for app in apps:
                if app.get("name") == app_name:
                    return app.get("trackingUrl")

            self.log.info(f"YARN에서 앱 이름 '{app_name}'을(를) 찾을 수 없습니다.")
            return None

        except requests.RequestException as e:
            self.log.error(f"YARN API 호출 중 오류 발생: {e}")
            return None

    def _build_blocks(
        self, dag_id: str, task_id: str, execution_date: str, message: str, log_url: str | None = None
    ) -> list[object]:
        """
        Slack Block Kit을 사용하여 메시지 레이아웃을 생성합니다.
        """
        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": "🚨 Airflow Task Failed", "emoji": True}},
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG ID:*\n{dag_id}"},
                    {"type": "mrkdwn", "text": f"*Task ID:*\n{task_id}"},
                    {"type": "mrkdwn", "text": f"*Execution Date:*\n{execution_date}"},
                ],
            },
            {"type": "section", "text": {"type": "mrkdwn", "text": f"*Message:*\n{message}"}},
        ]

        if log_url:
            blocks.append(
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "View YARN Logs", "emoji": True},
                            "url": log_url,
                            "style": "danger",
                        }
                    ],
                }
            )

        return blocks

    def send_alert(self, context: Context, message: str = ""):
        """
        기본적인 실패 알림을 전송합니다.
        """
        self._send(context, message)

    def send_spark_failure_alert(self, context: Context):
        """
        Spark 작업 실패 시 YARN 로그 URL을 포함하여 알림을 전송합니다.
        """
        dag_id = context.get("task_instance").dag_id

        # YARN 애플리케이션 URL 조회
        log_url = self._get_yarn_application_url(dag_id)

        message = "Spark 작업이 실패했습니다."
        if not log_url:
            message += "\n(YARN 애플리케이션 로그를 찾을 수 없습니다.)"

        self._send(context, message, log_url)

    def _send(self, context: Context, message: str, log_url: str | None = None):
        """
        실제 Slack 메시지를 전송하는 내부 메서드입니다.
        """
        try:
            ti = context.get("task_instance")
            dag_id = ti.dag_id
            task_id = ti.task_id
            # Airflow 버전에 따라 logical_date 또는 execution_date 사용
            execution_date = context.get("logical_date") or context.get("execution_date")
            formatted_date = execution_date.strftime("%Y-%m-%d %H:%M:%S") if execution_date else "N/A"

            env = Variable.get("ENV", default_var="DEV").upper()
            full_message = f"[{env}] {message}"

            blocks = self._build_blocks(dag_id, task_id, formatted_date, full_message, log_url)

            # SlackWebhookHook을 사용하여 메시지 전송
            hook = SlackWebhookHook(slack_webhook_conn_id=self.slack_conn_id)
            hook.send(text=full_message, blocks=blocks)

            self.log.info(f"Slack 알림 전송 완료: {dag_id}.{task_id}")

        except Exception as e:
            self.log.error(f"Slack 알림 전송 실패: {e}")
            # 알림 실패가 태스크 실패로 이어지지 않도록 예외를 무시합니다.
