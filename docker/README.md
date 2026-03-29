### Worker 노드 확장

Worker 전용 노드를 추가하여 태스크 실행 용량을 확장할 수 있습니다. NFS로 마스터의 프로젝트 디렉토리를 공유하고, Worker는 마스터의 DB/Redis에 원격 접속합니다.

**아키텍처**

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

**방화벽 설정**

모든 연결은 **워커 → 마스터** 단방향입니다. 마스터 측 인바운드를 열어줍니다.

| 포트 | 프로토콜 | 용도                             | 방향          |
| ---- | -------- | -------------------------------- | ------------- |
| 2049 | TCP      | NFS v4                           | 워커 → 마스터 |
| 5432 | TCP      | PostgreSQL (Airflow metadata DB) | 워커 → 마스터 |
| 6379 | TCP      | Redis (Celery broker)            | 워커 → 마스터 |

> NFS v4는 2049 단일 포트로 동작합니다. v3 이하를 사용하는 경우 rpcbind(111), mountd, statd 등 추가 포트가 필요합니다.

**마스터 노드: NFS 설정**

Amazon Linux 2023:

```bash
sudo dnf install -y nfs-utils
echo "/path/to/project  워커IP(rw,sync,no_subtree_check,no_root_squash)" | sudo tee -a /etc/exports
sudo systemctl enable --now nfs-server
sudo exportfs -arv
```

Ubuntu/Debian:

```bash
sudo apt install -y nfs-kernel-server
echo "/path/to/project  워커IP(rw,sync,no_subtree_check,no_root_squash)" | sudo tee -a /etc/exports
sudo exportfs -arv
```

> EC2 환경에서는 firewalld 대신 Security Group에서 인바운드 2049, 5432, 6379 TCP를 워커 IP에 대해 허용합니다.

**워커 노드: NFS 마운트 및 실행**

Amazon Linux 2023:

```bash
sudo dnf install -y nfs-utils
sudo mkdir -p /opt/airflow/project
sudo mount -t nfs4 마스터IP:/path/to/project /opt/airflow/project

# 영구 마운트 (/etc/fstab)
echo "마스터IP:/path/to/project  /opt/airflow/project  nfs4  defaults,_netdev,noresvport  0  0" | sudo tee -a /etc/fstab
```

Ubuntu/Debian:

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

### Airflow 2.x vs 3.x

`compose.yml`에 공통 설정을 두고, 버전별 오버라이드 파일로 차이점을 관리합니다.

**서비스 구성**

| 서비스        | 2.x (`compose.v2.yml`) | 3.x (`compose.v3.yml`)         |
| ------------- | ---------------------- | ------------------------------ |
| Web UI        | `airflow-webserver`    | `airflow-apiserver`            |
| DAG Processor | scheduler에 내장       | `airflow-dag-processor` (별도) |
| Scheduler     | 공통                   | 공통                           |
| Worker        | 공통                   | + apiserver 의존               |
| Triggerer     | 공통                   | 공통                           |

**환경변수**

| 설정          | 2.x                           | 3.x                                       |
| ------------- | ----------------------------- | ----------------------------------------- |
| 인증          | `AIRFLOW__API__AUTH_BACKENDS` | `AIRFLOW__CORE__AUTH_MANAGER`             |
| Execution API | 없음                          | `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` |
| JWT           | 없음                          | `AIRFLOW__API_AUTH__JWT_SECRET/ISSUER`    |

**헬스체크**

| 서비스    | 2.x              | 3.x                      |
| --------- | ---------------- | ------------------------ |
| Web UI    | `/health`        | `/api/v2/monitor/health` |
| Scheduler | `/health` (동일) | `/health` (동일)         |

**Airflow 설정 방식**

`airflow.cfg` 파일 마운트 대신 `AIRFLOW__SECTION__KEY` 환경변수로 모든 설정을 관리합니다. 버전 공통 설정은 `compose.yml`에, 버전별 설정은 각 오버라이드 파일에 정의되어 있습니다.
