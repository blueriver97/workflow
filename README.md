# Airflow Workflow Project

Apache Airflow 기반 데이터 파이프라인 워크플로우 저장소. Docker Compose로 로컬/운영 환경을 구성하고, Spark on YARN으로 배치 및 스트리밍 작업을 수행합니다.

## 프로젝트 구조

```
.
├── compose.yml                 # 공통 Docker Compose (PostgreSQL, Redis, Scheduler, Worker, Triggerer)
├── compose.v2.yml              # Airflow 2.x 오버라이드 (webserver)
├── compose.v3.yml              # Airflow 3.x 오버라이드 (apiserver, dag-processor)
├── compose.worker.yml          # Worker 전용 노드 오버라이드
├── build.yml                   # Docker 이미지 빌드
├── docker/                     # Dockerfile, Hadoop/Spark 설정, 바이너리
├── dags/                       # DAG 정의 및 설정
├── src/                        # Spark 애플리케이션
├── plugins/                    # 커스텀 Operator, 알림
├── fixtures/                   # Airflow connections/variables 백업/복원
├── scripts/                    # 개발 환경 셋업, Git 훅
├── pyproject.toml
└── .env
```

자세한 내용은 각 디렉토리의 README를 참고하세요:

- [src/README.md](src/README.md) — Spark 애플리케이션, CDC Watermark, Settings
- [dags/README.md](dags/README.md) — DAG 목록 및 설정
- [plugins/README.md](plugins/README.md) — StreamingSparkSubmitOperator, SlackNotifier
- [docker/README.md](docker/README.md) — Worker 확장, NFS, Airflow 2.x vs 3.x

## 기술 스택

| 구분              | 기술                           | 버전           |
| ----------------- | ------------------------------ | -------------- |
| Orchestration     | Apache Airflow                 | 2.11.2 / 3.1.5 |
| Compute           | Apache Spark on YARN           | 4.0.1          |
| Storage Format    | Apache Iceberg                 | v2             |
| Catalog           | AWS Glue, Apache Polaris       | -              |
| Messaging         | Apache Kafka (Debezium CDC)    | -              |
| Transformation    | dbt-core + dbt-athena + Cosmos | >=1.11.6       |
| Lineage           | OpenLineage + DataHub          | -              |
| Secret Management | HashiCorp Vault                | -              |
| Source DB         | MySQL, SQL Server              | -              |
| Infra             | Docker Compose, NFS            | -              |
| Runtime           | Python 3.12, Java 17 (OpenJDK) | -              |

## 시작하기

### 1. 사전 요구 사항

- Docker & Docker Compose
- Java 17 (로컬 개발 시)
- Python 3.12 (로컬 개발 시)

### 2. 환경 설정

프로젝트 루트에 `.env` 파일을 생성합니다.

```bash
# 의존성
SPARK_VERSION=4.0.1
HADOOP_VERSION=3.4.2

# AWS
AWS_REGION=ap-northeast-2
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_ENDPOINT_URL_S3=https://s3.ap-northeast-2.amazonaws.com
S3_BUCKET=s3a://your-bucket

# YARN
YARN_RM_HOSTNAME_RM01=node01
YARN_RM_HOSTNAME_RM02=node02
YARN_RM_ADDRESS_RM01=node01:8032
YARN_RM_ADDRESS_RM02=node02:8032
YARN_RM_SCHEDULER_ADDRESS_RM01=node01:8030
YARN_RM_SCHEDULER_ADDRESS_RM02=node02:8030
ARCH=aarch64
```

### 3. 바이너리 다운로드 및 이미지 빌드

```bash
# Spark, Hadoop JAR 다운로드
cd docker/download && bash download.sh

# Docker 이미지 빌드
docker-compose -f build.yml build --no-cache
```

### 4. 실행

```bash
# Airflow 2.x (마스터 노드)
docker-compose -f compose.yml -f compose.v2.yml up -d

# Airflow 3.x (마스터 노드)
docker-compose -f compose.yml -f compose.v3.yml up -d

# 종료
docker-compose -f compose.yml -f compose.v2.yml down

# 로그 확인
docker-compose -f compose.yml -f compose.v2.yml logs -f airflow-scheduler
```

실행 후 http://localhost:8080 에서 Airflow UI에 접속할 수 있습니다. (기본 계정: `airflow` / `airflow`)

## Fixtures (백업/복원)

Airflow connections 및 variables를 JSON으로 백업/복원합니다. Airflow 2(webserver)/3(apiserver) 컨테이너를 자동 감지합니다.

```bash
# 백업
bash fixtures/backup_conn.sh
bash fixtures/backup_vars.sh

# 복원
bash fixtures/restore_conn.sh
bash fixtures/restore_vars.sh
```

## Data Lineage

Spark 작업의 입출력 데이터셋 정보를 OpenLineage Spark Listener로 수집합니다. dbt 작업은 dbt-ol을 통해 OpenLineage 이벤트를 Kafka로 전송하고, DataHub가 이를 소비하여 리니지를 시각화합니다.

## AWS Glue Catalog 관리 (CLI)

AWS CLI로 Glue Catalog의 데이터베이스/테이블을 조회하고 삭제할 수 있습니다.

```bash
# 데이터베이스 목록
aws glue get-databases --profile default

# 특정 데이터베이스의 테이블 목록
aws glue get-tables --database-name store_bronze --profile default

# 특정 테이블 상세 정보
aws glue get-table --database-name store_bronze --name tb_lower --profile default

# 테이블 삭제
aws glue delete-table --database-name store_bronze --name tb_lower --profile default

# 테이블 여러 개 한번에 삭제
aws glue batch-delete-table --database-name store_bronze \
  --tables-to-delete tb_lower TB_UPPER TB_COMPOSITE_KEY --profile default

# 데이터베이스 삭제 (테이블이 비어있어야 함)
aws glue delete-database --name store_bronze --profile default
```

> 출력 포맷: `--output yaml` (AWS CLI v2), `--query "TableList[].Name" --output text` (v1/v2 공통)

## 라이선스

이 프로젝트는 Apache License 2.0을 따릅니다.
