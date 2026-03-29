### Spark 애플리케이션

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

### CDC Watermark

Kafka CDC 파이프라인은 토픽별 처리 상태를 `ops_bronze.cdc_watermark` Iceberg 테이블에 기록합니다.

**Append-only 설계**

이 테이블은 MERGE INTO 대신 **INSERT(append)** 방식으로 기록합니다.

Iceberg의 optimistic concurrency 모델에서 MERGE는 read-modify-write 연산이므로, 동시에 여러 writer가 커밋하면 스냅샷 충돌(`ValidationException: Found conflicting files`)이 발생합니다. 반면 append는 기존 데이터를 읽지 않고 새 파일만 추가하므로, 동시 커밋 시 Iceberg가 자동으로 rebase하여 충돌 없이 처리됩니다.

이 설계가 필요한 이유:

- **프로세스 내**: 하나의 spark-submit에서 멀티스레드로 여러 토픽을 병렬 처리하며, 각 스레드가 배치 완료 시 동일 테이블에 기록
- **프로세스 간**: 여러 DAG가 각각 별도 spark-submit으로 동시 실행되어 같은 테이블에 기록

최신 상태 조회 시 `(dag_id, bronze_schema, table_name)` 기준 `processed_at DESC`로 첫 번째 행을 사용합니다.

코드에는 `append_watermark()`와 `merge_watermark()` 두 함수가 모두 구현되어 있으며, 현재 파이프라인에서는 `append_watermark()`를 사용합니다.

### Settings (Pydantic)

`utils/settings.py`에서 환경변수와 Vault 시크릿을 통합 관리합니다.

```
Settings
├── VaultSettings (url, username, password, secret_path)
├── DatabaseSettings (type, host, port, user, password) ← Vault에서 주입
├── AwsSettings (profile, catalog, s3_bucket, iceberg_path)
└── KafkaSettings (bootstrap_servers, schema_registry, ...) [optional]
```

DAG에서 Spark 앱으로 환경변수를 전달할 때 Jinja 템플릿(`{{ var.value.* }}`)을 사용하여 DAG 파싱 시점의 DB 조회를 제거합니다.

### utils/

| 파일               | 설명                                                                                |
| ------------------ | ----------------------------------------------------------------------------------- |
| `settings.py`      | Pydantic BaseSettings + Vault 연동 (`env_file=".env"`, `env_nested_delimiter="__"`) |
| `database.py`      | JDBC 매니저 — `BaseDatabaseManager`, `MySQLManager`, `SQLServerManager`             |
| `listener.py`      | Spark Structured Streaming `BatchProgressListener` (시그널 감지)                    |
| `signal.py`        | S3 시그널 기반 graceful shutdown (`build_signal_path`, `check_stop_signal`)         |
| `spark_logging.py` | Log4j 기반 Spark 로거 (`SparkLoggerManager` 싱글턴)                                 |
