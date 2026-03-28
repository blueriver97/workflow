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
├── docker/
│   ├── Dockerfile              # Multi-stage 빌드 (Spark/Hadoop/Java 17 포함)
│   ├── requirements.txt
│   ├── setup.sh                # 런타임 환경변수 치환 (Hadoop/Spark 설정)
│   ├── config/
│   │   ├── hadoop/             # core-site.xml, yarn-site.xml 등
│   │   └── spark/              # spark-defaults.conf
│   └── download/               # Spark/Hadoop 바이너리 및 JDBC 드라이버
├── dags/
│   ├── spark/                  # Spark 기반 DAG
│   │   ├── glue_kafka_to_iceberg.py
│   │   ├── glue_mysql_to_iceberg.py
│   │   ├── glue_sqlserver_to_iceberg.py
│   │   ├── glue_watermark_maintenance.py
│   │   └── polaris_kafka_to_iceberg.py
│   ├── dbt/
│   │   └── store_mart.py       # dbt + Cosmos DAG (Athena 타겟)
│   └── configs/                # DAG별 YAML 설정
│       ├── glue_kafka_to_iceberg.yml
│       ├── glue_mysql_to_iceberg.yml
│       ├── glue_sqlserver_to_iceberg.yml
│       ├── glue_watermark_maintenance.yml
│       └── polaris_kafka_to_iceberg.yml
├── src/                        # Spark 애플리케이션
│   ├── kafka_to_iceberg.py     # Kafka CDC 스트리밍 적재
│   ├── mysql_to_iceberg.py     # MySQL 배치 적재
│   ├── sqlserver_to_iceberg.py # SQL Server 배치 적재
│   ├── watermark_maintenance.py # CDC watermark 테이블 정리
│   └── utils/
│       ├── settings.py         # Pydantic 설정 + Vault 연동
│       ├── database.py         # JDBC 매니저 (MySQL, SQL Server)
│       ├── listener.py         # Spark Structured Streaming 리스너
│       ├── signal.py           # S3 시그널 파일 (graceful shutdown)
│       └── spark_logging.py    # Log4j 기반 Spark 로거
├── plugins/
│   ├── operators/
│   │   └── custom_spark.py     # StreamingSparkSubmitOperator, LineageMappedSparkSubmitOperator
│   └── alerts/
│       ├── abstract_notifier.py
│       └── slack_notifier.py   # Redis 기반 Slack 스레드 알림
├── fixtures/                   # Airflow connections/variables 백업/복원 (AF2/AF3 호환)
├── scripts/                    # 개발 환경 셋업, Git 훅, 커밋 검증
├── pyproject.toml
└── .env
```

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

## DAG 목록

### Spark DAG

| DAG                          | 유형           | 소스                 | 설명                                                                |
| ---------------------------- | -------------- | -------------------- | ------------------------------------------------------------------- |
| `glue_kafka_to_iceberg`      | 스트리밍       | Kafka (Debezium CDC) | Kafka 토픽별 멀티스레드 병렬 처리, S3 시그널 기반 graceful shutdown |
| `glue_mysql_to_iceberg`      | 배치           | MySQL (JDBC)         | 테이블별 dynamic task mapping으로 병렬 spark-submit                 |
| `glue_sqlserver_to_iceberg`  | 배치           | SQL Server (JDBC)    | 3-part 테이블명 (`db.schema.table`) 지원                            |
| `polaris_kafka_to_iceberg`   | 스트리밍       | Kafka (Debezium CDC) | Glue 대신 Polaris REST Catalog 사용                                 |
| `glue_watermark_maintenance` | 배치 (@weekly) | -                    | CDC watermark 테이블 정리 및 Iceberg 최적화                         |

### dbt DAG

| DAG          | 설명                                                                                                              |
| ------------ | ----------------------------------------------------------------------------------------------------------------- |
| `store_mart` | Cosmos 기반 dbt 실행 (compile → run → docs → source freshness), Athena 타겟, OpenLineage → Kafka → DataHub 리니지 |

### DAG 설정

각 DAG는 `dags/configs/{DAG_ID}.yml` 파일에서 설정을 로드합니다.

```yaml
# glue_kafka_to_iceberg.yml
job:
  concurrency: 3
  topics:
    - local.store.tb_lower
    - local.store.TB_UPPER

# glue_mysql_to_iceberg.yml
job:
  num_partition: 1
  database: store
  tables:
    - store.tb_lower
    - store.TB_UPPER

# glue_watermark_maintenance.yml
job:
  retention_days: 7
```

## Spark 애플리케이션

### kafka_to_iceberg.py

Kafka CDC 스트리밍 파이프라인. 토픽별 독립 스트림을 멀티스레드로 병렬 처리합니다.

- **Debezium CDC 처리**: Avro 역직렬화, Schema Registry 연동, 타입 변환 (DATE/TIME/TIMESTAMP)
- **Iceberg 적재**: PK 기반 중복 제거 → MERGE INTO (upsert) + DELETE (삭제 이벤트)
- **Watermark 기록**: 토픽별 처리 상태를 `ops_bronze.cdc_watermark` 테이블에 append
- **Graceful Shutdown**: S3 시그널 파일 폴링으로 진행 중인 마이크로 배치 완료 후 종료
- **동시성 제어**: `--concurrency` 인자와 `threading.Semaphore`로 동시 처리 토픽 수 제한

### mysql_to_iceberg.py / sqlserver_to_iceberg.py

JDBC 배치 적재 파이프라인. 소스 테이블을 읽어 Iceberg 테이블로 전체 교체(RTAS)합니다.

- **파티셔닝**: auto_increment/identity 컬럼 자동 감지, bounds 쿼리로 분산 읽기
- **PK 처리**: INFORMATION_SCHEMA에서 PK 조회 → `id_iceberg` (MD5 해시) 생성
- **CHAR Trim**: MySQL/SQL Server의 CHAR 고정길이 공백 제거
- **Vault 연동**: DB 접속 정보를 HashiCorp Vault에서 런타임 조회

### watermark_maintenance.py

CDC watermark 테이블의 주기적 정리. `--catalog`, `--warehouse`, `--retention-days` CLI 인자를 받으며 Settings 의존 없이 독립 실행됩니다.

1. 보관 기간 초과 레코드 삭제 (키별 최신 1건 보존)
2. `rewrite_data_files` (소파일 compaction)
3. `expire_snapshots` (오래된 스냅샷 만료, `retain_last=5`)
4. `remove_orphan_files` (참조 해제된 파일 삭제)

## CDC Watermark

Kafka CDC 파이프라인은 토픽별 처리 상태를 `ops_bronze.cdc_watermark` Iceberg 테이블에 기록합니다.

### Append-only 설계

이 테이블은 MERGE INTO 대신 **INSERT(append)** 방식으로 기록합니다.

Iceberg의 optimistic concurrency 모델에서 MERGE는 read-modify-write 연산이므로, 동시에 여러 writer가 커밋하면 스냅샷 충돌(`ValidationException: Found conflicting files`)이 발생합니다. 반면 append는 기존 데이터를 읽지 않고 새 파일만 추가하므로, 동시 커밋 시 Iceberg가 자동으로 rebase하여 충돌 없이 처리됩니다.

이 설계가 필요한 이유:

- **프로세스 내**: 하나의 spark-submit에서 멀티스레드로 여러 토픽을 병렬 처리하며, 각 스레드가 배치 완료 시 동일 테이블에 기록
- **프로세스 간**: 여러 DAG가 각각 별도 spark-submit으로 동시 실행되어 같은 테이블에 기록

최신 상태 조회 시 `(dag_id, bronze_schema, table_name)` 기준 `processed_at DESC`로 첫 번째 행을 사용합니다.

코드에는 `append_watermark()`와 `merge_watermark()` 두 함수가 모두 구현되어 있으며, 현재 파이프라인에서는 `append_watermark()`를 사용합니다.

## 플러그인

### StreamingSparkSubmitOperator

`SparkSubmitOperator`를 확장하여 S3 시그널 기반 graceful shutdown을 지원합니다.

- `execute()`: 잔여 시그널 파일 정리 후 spark-submit 실행
- `on_kill()`: S3에 시그널 파일 생성 → Spark 앱이 감지하여 현재 배치 완료 후 종료

### SlackNotifier

Redis 기반 Slack 스레드 알림 시스템.

- 실패/재시도/복구 상태별 Slack 스레드 메시지
- Redis SET NX로 중복 알림 방지
- 월별 실패 횟수 집계 (`airflow:alert:{YYYYMM}`)
- DAG 파싱 시 연결 비용 제거를 위한 lazy callback 패턴

## 설정 관리

### Settings (Pydantic)

`src/utils/settings.py`에서 환경변수와 Vault 시크릿을 통합 관리합니다.

```
Settings
├── VaultSettings (url, username, password, secret_path)
├── DatabaseSettings (type, host, port, user, password) ← Vault에서 주입
├── AwsSettings (profile, catalog, s3_bucket, iceberg_path)
└── KafkaSettings (bootstrap_servers, schema_registry, ...) [optional]
```

DAG에서 Spark 앱으로 환경변수를 전달할 때 Jinja 템플릿(`{{ var.value.* }}`)을 사용하여 DAG 파싱 시점의 DB 조회를 제거합니다.

## Worker 노드 확장

Worker 전용 노드를 추가하여 태스크 실행 용량을 확장할 수 있습니다. NFS로 마스터의 프로젝트 디렉토리를 공유하고, Worker는 마스터의 DB/Redis에 원격 접속합니다.

### 아키텍처

```
┌─────────────────────────┐       ┌─────────────────────────┐
│       마스터 노드         │       │       워커 노드          │
│                         │       │                         │
│  scheduler              │       │  worker                 │
│  webserver / apiserver  │       │                         │
│  triggerer              │       │                         │
│  postgres  ◄────────────┼───────┼── (5432/tcp)            │
│  redis     ◄────────────┼───────┼── (6379/tcp)            │
│                         │       │                         │
│  NFS export ◄───────────┼───────┼── (2049/tcp)            │
│  /path/to/project       │       │  /opt/airflow/project   │
└─────────────────────────┘       └─────────────────────────┘
```

### 방화벽 설정

모든 연결은 **워커 → 마스터** 단방향입니다. 마스터 측 인바운드를 열어줍니다.

| 포트 | 프로토콜 | 용도                             | 방향          |
| ---- | -------- | -------------------------------- | ------------- |
| 2049 | TCP      | NFS v4                           | 워커 → 마스터 |
| 5432 | TCP      | PostgreSQL (Airflow metadata DB) | 워커 → 마스터 |
| 6379 | TCP      | Redis (Celery broker)            | 워커 → 마스터 |

> NFS v4는 2049 단일 포트로 동작합니다. v3 이하를 사용하는 경우 rpcbind(111), mountd, statd 등 추가 포트가 필요합니다.

### 마스터 노드: NFS 설정

**Amazon Linux 2023**

```bash
sudo dnf install -y nfs-utils
echo "/path/to/project  워커IP(rw,sync,no_subtree_check,no_root_squash)" | sudo tee -a /etc/exports
sudo systemctl enable --now nfs-server
sudo exportfs -arv
```

**Ubuntu/Debian**

```bash
sudo apt install -y nfs-kernel-server
echo "/path/to/project  워커IP(rw,sync,no_subtree_check,no_root_squash)" | sudo tee -a /etc/exports
sudo exportfs -arv
```

> EC2 환경에서는 firewalld 대신 Security Group에서 인바운드 2049, 5432, 6379 TCP를 워커 IP에 대해 허용합니다.

### 워커 노드: NFS 마운트 및 실행

**Amazon Linux 2023**

```bash
sudo dnf install -y nfs-utils
sudo mkdir -p /opt/airflow/project
sudo mount -t nfs4 마스터IP:/path/to/project /opt/airflow/project

# 영구 마운트 (/etc/fstab)
echo "마스터IP:/path/to/project  /opt/airflow/project  nfs4  defaults,_netdev,noresvport  0  0" | sudo tee -a /etc/fstab
```

**Ubuntu/Debian**

```bash
sudo apt install -y nfs-common
sudo mkdir -p /opt/airflow/project
sudo mount -t nfs4 마스터IP:/path/to/project /opt/airflow/project

# 영구 마운트 (/etc/fstab)
echo "마스터IP:/path/to/project  /opt/airflow/project  nfs4  defaults,_netdev,noresvport  0  0" | sudo tee -a /etc/fstab
```

> `_netdev`: 네트워크 활성화 후 마운트 보장. `noresvport`: 재연결 시 포트 재사용 허용 (AWS 환경 권장).

워커 노드 `.env` 파일:

```bash
# 마스터 노드 접속 정보
MASTER_HOST=192.168.x.x

# NFS 마운트된 프로젝트 경로
AIRFLOW_PROJ_DIR=/opt/airflow/project
```

워커 실행:

```bash
# Airflow 2.x 워커
docker-compose -f compose.yml -f compose.v2.yml -f compose.worker.yml up -d

# Airflow 3.x 워커
docker-compose -f compose.yml -f compose.v3.yml -f compose.worker.yml up -d

# 워커 스케일링 (여러 프로세스)
docker-compose -f compose.yml -f compose.v2.yml -f compose.worker.yml up -d --scale airflow-worker=3
```

## Airflow 2.x vs 3.x

`compose.yml`에 공통 설정을 두고, 버전별 오버라이드 파일로 차이점을 관리합니다.

### 서비스 구성

| 서비스        | 2.x (`compose.v2.yml`) | 3.x (`compose.v3.yml`)         |
| ------------- | ---------------------- | ------------------------------ |
| Web UI        | `airflow-webserver`    | `airflow-apiserver`            |
| DAG Processor | scheduler에 내장       | `airflow-dag-processor` (별도) |
| Scheduler     | 공통                   | 공통                           |
| Worker        | 공통                   | + apiserver 의존               |
| Triggerer     | 공통                   | 공통                           |

### 환경변수

| 설정          | 2.x                           | 3.x                                       |
| ------------- | ----------------------------- | ----------------------------------------- |
| 인증          | `AIRFLOW__API__AUTH_BACKENDS` | `AIRFLOW__CORE__AUTH_MANAGER`             |
| Execution API | 없음                          | `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` |
| JWT           | 없음                          | `AIRFLOW__API_AUTH__JWT_SECRET/ISSUER`    |

### 헬스체크

| 서비스    | 2.x              | 3.x                      |
| --------- | ---------------- | ------------------------ |
| Web UI    | `/health`        | `/api/v2/monitor/health` |
| Scheduler | `/health` (동일) | `/health` (동일)         |

### Airflow 설정 방식

`airflow.cfg` 파일 마운트 대신 `AIRFLOW__SECTION__KEY` 환경변수로 모든 설정을 관리합니다. 버전 공통 설정은 `compose.yml`에, 버전별 설정은 각 오버라이드 파일에 정의되어 있습니다.

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

## 라이선스

이 프로젝트는 Apache License 2.0을 따릅니다.
