from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from alerts.slack_notifier import SlackNotifier
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, InvocationMode
from cosmos.operators.local import (
    DbtDocsLocalOperator,
    DbtSourceLocalOperator,
)
from operators.dbt_compile import DbtCompileLocalOperatorNoUpload

DAG_ID = Path(__file__).name.removesuffix(".py")

# dbt 프로젝트 경로 설정 (다른 서버/노드 전송 시 이 경로만 변경)
DBT_ROOT_DIR = Path("/opt/airflow/dbt")
DBT_PROJECT_DIR = DBT_ROOT_DIR / "store_mart"
DBT_EXECUTABLE_PATH = "/home/airflow/.local/bin/dbt"
DBT_OL_EXECUTABLE_PATH = "/home/airflow/.local/bin/dbt-ol"

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

# dbt-ol (SUBPROCESS) → openlineage.yml (Kafka transport) → datahub-toolkit에서 schemaField 보강
# Airflow 3 task 프로세스는 DB 직접 접근을 차단 (sql_alchemy_conn=airflow-db-not-allowed:///)
# dbt-ol의 Kafka transport가 airflow를 import할 때 DB 초기화 crash 방지용 더미 URL 필요
OPENLINEAGE_CONFIG = str(DBT_PROJECT_DIR / "openlineage.yml")

dbt_run_execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path=DBT_OL_EXECUTABLE_PATH,
    invocation_mode=InvocationMode.SUBPROCESS,
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
    compile = DbtCompileLocalOperatorNoUpload(
        task_id="dbt_compile",
        project_dir=DBT_PROJECT_DIR,
        profile_config=profile_config,
        dbt_executable_path=DBT_EXECUTABLE_PATH,
        extra_context={"dbt_dag_task_group_identifier": DAG_ID},
    )

    # 2. dbt-ol run: 모델별 실행 + OpenLineage 이벤트 Kafka 게시
    dbt_run = DbtTaskGroup(
        group_id="store_mart",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_DIR,
            env_vars={
                "OPENLINEAGE_CONFIG": OPENLINEAGE_CONFIG,
                "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "sqlite:////tmp/airflow_dbt_dummy.db",
            },
        ),
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
