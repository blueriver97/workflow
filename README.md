# Airflow Workflow Project

이 프로젝트는 Apache Airflow를 기반으로 데이터 파이프라인을 구축하고 관리하기 위한 워크플로우 저장소입니다. Docker Compose를 사용하여 로컬 개발 환경을 구성하고, Spark, Hadoop(YARN), Glue, Polaris, DataHub(OpenLineage) 등 다양한 데이터 에코시스템과 연동하여 배치 및 스트리밍 작업을 수행합니다.

## 프로젝트 구조

- **compose.yml**: 공통 Docker Compose 설정 (버전 무관)
- **compose.v2.yml**: Airflow 2.x 오버라이드
- **compose.v3.yml**: Airflow 3.x 오버라이드
- **compose.worker.yml**: Worker 전용 노드 오버라이드
- **build.yml**: Docker 이미지 빌드 전용
- **docker/**: Dockerfile, requirements, Spark/Hadoop 바이너리 및 설정
- **dags/**: Airflow DAG 파일
- **src/**: Spark 애플리케이션 소스 코드 및 유틸리티
- **plugins/**: Airflow 플러그인
- **fixtures/**: Airflow connections/variables 백업 및 복원
- **scripts/**: 개발 및 유지보수 스크립트

## 시작하기

### 1. 사전 요구 사항

- Docker & Docker Compose
- Java 17 (로컬 개발 시)
- Python 3.12 (로컬 개발 시)

### 2. 환경 설정

프로젝트 루트에 `.env` 파일을 생성하고 필요한 설정을 진행합니다.

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

# Yarn
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

## 주요 기능 및 DAG

### Glue Catalog 연동

Kafka 토픽 및 MySQL 테이블 데이터를 읽어 AWS Glue Catalog 기반의 Iceberg 테이블로 적재하는 배치 및 스트리밍 작업을 수행합니다.

### Polaris Catalog 연동

Kafka 토픽 및 MySQL 테이블 데이터를 읽어 Apache Polaris Catalog 기반의 Iceberg 테이블로 적재하는 작업을 수행합니다.

### Data Lineage

Airflow에서 실행되는 Spark 작업의 입출력 데이터셋 정보를 OpenLineage를 통해 수집하고, DataHub로 전송하여 데이터 리니지를 시각화합니다.

## 기술 스택

- **Orchestration**: Apache Airflow
- **Compute**: Apache Spark (on YARN)
- **Storage Format**: Apache Iceberg
- **Catalog**: AWS Glue, Apache Polaris
- **Message Queue**: Apache Kafka
- **Metadata & Lineage**: DataHub, OpenLineage
- **Secret Management**: HashiCorp Vault
- **Database**: MySQL, PostgreSQL

## 개발 가이드

### Spark 애플리케이션 개발

`src/` 디렉토리 내에서 Spark 작업을 개발하며, 공통 유틸리티 모듈을 활용합니다. 배포 시에는 유틸리티 모듈을 패키징하여 함께 제출합니다.

### Airflow DAG 개발

`dags/` 디렉토리 내에서 DAG를 정의하며, `SparkSubmitOperator`를 사용하여 Spark 작업을 실행하도록 구성합니다.

## 라이선스

이 프로젝트는 Apache License 2.0을 따릅니다.
