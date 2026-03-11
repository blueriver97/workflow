import shutil
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from alerts.slack_notifier import SlackNotifier
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode
from cosmos.operators.local import (
    DbtCompileLocalOperator,
    DbtDocsLocalOperator,
    DbtSourceLocalOperator,
)

DAG_ID = Path(__file__).name.removesuffix(".py")

# dbt 프로젝트 경로 설정 (다른 서버/노드 전송 시 이 경로만 변경)
DBT_ROOT_DIR = Path("/opt/airflow/dbt")
DBT_PROJECT_DIR = DBT_ROOT_DIR / "store_mart"
DBT_EXECUTABLE_PATH = "/home/airflow/.local/bin/dbt"
DBT_OL_EXECUTABLE_PATH = "/home/airflow/.local/bin/dbt-ol"

# dbt-ol이 설치되어 있으면 우선 사용, 없으면 dbt로 폴백
DBT_RUN_EXECUTABLE_PATH = DBT_OL_EXECUTABLE_PATH if shutil.which("dbt-ol") else DBT_EXECUTABLE_PATH

slack_notifier = SlackNotifier(
    channel="#data-alerts", conn_id="slack_api", redis_host="redis", redis_port=6379, redis_db=0
)

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

profile_config = ProfileConfig(
    profile_name="athena",
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT_DIR / "profiles.yml",
)

# dbt-ol 우선, 미설치 시 dbt 폴백 (openlineage.yml의 Kafka transport 참조)
dbt_run_execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path=DBT_RUN_EXECUTABLE_PATH,
)

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="dbt store_mart: bronze → silver → gold transformation on Athena",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    on_failure_callback=slack_notifier.send_failure,
    on_success_callback=slack_notifier.send_recovery,
) as dag:
    # 1. compile: SQL 유효성 검증 (target/compiled 생성)
    compile = DbtCompileLocalOperator(
        task_id="dbt_compile",
        project_dir=DBT_PROJECT_DIR,
        profile_config=profile_config,
        dbt_executable_path=DBT_EXECUTABLE_PATH,
    )

    # 2. dbt-ol run: 모델별 실행 + OpenLineage 이벤트 Kafka 게시
    dbt_run = DbtTaskGroup(
        group_id="store_mart",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=dbt_run_execution_config,
        render_config=RenderConfig(
            select=["path:models"],
        ),
    )

    # 3. docs generate: 문서 및 카탈로그 생성 (target/catalog.json, target/index.html)
    docs_generate = DbtDocsLocalOperator(
        task_id="dbt_docs_generate",
        project_dir=DBT_PROJECT_DIR,
        profile_config=profile_config,
        dbt_executable_path=DBT_EXECUTABLE_PATH,
    )

    # 4. source freshness: 원천 데이터 신선도 검증
    source_freshness = DbtSourceLocalOperator(
        task_id="dbt_source_freshness",
        project_dir=DBT_PROJECT_DIR,
        profile_config=profile_config,
        dbt_executable_path=DBT_EXECUTABLE_PATH,
    )

    # 5. 산출물 전송 (다른 서버/노드로 배포 시 활성화)
    # transfer_artifacts = BashOperator(
    #     task_id="transfer_artifacts",
    #     bash_command=(
    #         # scp 전송
    #         # f"scp -r {DBT_PROJECT_DIR}/target user@remote-host:/opt/dbt/store_mart/target"
    #         # rsync 전송
    #         # f"rsync -avz {DBT_PROJECT_DIR}/target/ user@remote-host:/opt/dbt/store_mart/target/"
    #         # S3 업로드
    #         # f"aws s3 sync {DBT_PROJECT_DIR}/target s3://bucket/dbt/store_mart/target --delete"
    #     ),
    # )

    compile >> dbt_run >> [docs_generate, source_freshness]
    # docs_generate 이후 전송이 필요한 경우:
    # [docs_generate, source_freshness] >> transfer_artifacts
