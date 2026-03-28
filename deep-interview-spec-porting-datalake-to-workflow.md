# Deep Interview Spec: datalake-iceberg → workflow 기능 포팅

## Metadata

- Interview ID: port-datalake-iceberg-2026-03-28
- Rounds: 9
- Final Ambiguity Score: 19.2%
- Type: brownfield
- Generated: 2026-03-28
- Threshold: 20%
- Status: PASSED

## Clarity Breakdown

| Dimension          | Score | Weight | Weighted  |
| ------------------ | ----- | ------ | --------- |
| Goal Clarity       | 0.9   | 35%    | 0.315     |
| Constraint Clarity | 0.85  | 25%    | 0.213     |
| Success Criteria   | 0.7   | 25%    | 0.175     |
| Context Clarity    | 0.7   | 15%    | 0.105     |
| **Total Clarity**  |       |        | **0.808** |
| **Ambiguity**      |       |        | **19.2%** |

## Goal

`/Users/kimyj/workspace/datalake-iceberg/src`에서 6개 기능을 `/Users/kimyj/workspace/workflow`로 포팅한다. 각 기능은 workflow 프로젝트의 기존 패턴(DAG + src + yml)에 맞춰 작성하며, 일부는 기능 재정의를 포함한다.

### 포팅 대상 (6개)

| #   | 원본 파일                          | 포팅 후 기능                                                                | 산출물          |
| --- | ---------------------------------- | --------------------------------------------------------------------------- | --------------- |
| 1   | `schema_to_iceberg.py`             | **schema_sync** — 원천 DB ↔ Iceberg 스키마 비교 + 주석 동기화 (기능 재정의) | DAG + src + yml |
| 2   | `kafka_to_s3.py`                   | Kafka → Avro 역직렬화 → Parquet → S3 적재                                   | DAG + src + yml |
| 4   | `mssql_to_parquet.py`              | SQL Server → Parquet → S3 백업 적재                                         | DAG + src + yml |
| 5   | `mysql_to_parquet.py`              | MySQL → Parquet → S3 백업 적재                                              | DAG + src + yml |
| 6   | `parquet_to_iceberg.py`            | S3 Parquet → Iceberg 테이블 변환                                            | DAG + src + yml |
| 7   | `iceberg_maintenance_procedure.py` | 범용 Iceberg 데이터 테이블 maintenance (compaction + expire + orphan)       | DAG + src + yml |

### 제외

| #   | 원본 파일                   | 제외 사유                                                                                        |
| --- | --------------------------- | ------------------------------------------------------------------------------------------------ |
| 3   | `kafka_to_iceberg_batch.py` | workflow의 `kafka_to_iceberg.py`가 상위 호환 (multi-thread, graceful shutdown, append watermark) |

## Constraints

### 공통 패턴 (workflow 기존 규칙 준수)

- **DAG**: `dags/spark/glue_{name}.py` 형식. `SparkSubmitOperator` 사용. Slack notifier, OpenLineage/DataHub lineage 포함. `expand_kwargs`로 테이블별 병렬 실행.
- **Spark App**: `src/{name}.py`. `main(spark, config, app_args)` 진입점 + `__main__` 블록에서 SparkSession 생성. `utils.settings.Settings` 활용.
- **YAML Config**: `dags/configs/glue_{name}.yml`. `job:` 키 하위에 설정.
- **로깅**: `SparkLoggerManager` 싱글턴 사용.
- **데이터베이스 접근**: `utils.database.BaseDatabaseManager` 하위 클래스 활용.

### Parquet 저장 경로

- `s3://<bucket>/data/raw/<schema>/<table>`
- 버킷/경로는 `Settings`에 새 속성 추가 (예: `RAW_WAREHOUSE` 또는 적절한 이름)
- `WAREHOUSE` 패턴과 유사하게 `s3a://<bucket>/data/raw` 형태의 property로 제공

### Maintenance (#7)

- **구조**: 일 1회 독립 DAG (야간 유휴 시간대 권장)
- **주기**: compaction + snapshot expire + orphan cleanup **모두 매일**
- **대상**: YAML에 스키마/테이블 목록 지정
- **실행 순서**: 테이블별 순차 (rewrite_data_files → expire_snapshots → remove_orphan_files)
- **기존 `watermark_maintenance.py`와 별도 유지** — 메타 테이블(cdc_watermark) vs 데이터 테이블 유지관리 분리

### Schema Sync (#1) — 기능 재정의

- **원본 `schema_to_iceberg.py`의 빈 테이블 생성 기능은 불필요** (RTAS로 자동 생성되므로)
- **새 기능**: 원천 DB ↔ Iceberg 스키마 비교 + 메타데이터 동기화
- **주기**: 일 1회
- **비교 항목**:
  1. 컬럼 수/순서/데이터타입 비교 (DB→Spark 변환규칙 기반 매칭) + **원천에는 있으나 Iceberg에 미반영된 컬럼 리포팅**
  2. 컬럼 주석(comment) 동기화 — 원천 → Iceberg (`ALTER TABLE ... ALTER COLUMN ... SET COMMENT`, 메타데이터 전용이라 CDC 스냅샷 충돌 없음)
  3. Nullable 정합성 비교 (원천 NOT NULL ↔ Iceberg required)
  4. PK 정합성 비교 (원천 PK ↔ Iceberg identifier fields)
  5. 테이블 주석 동기화 (원천 → Iceberg table property)
- **제외 항목**:
  - 신규 테이블 감지 (#6) — 제외
  - 삭제된 컬럼 감지 (#7) — CDC에서 자연 오류 발생하므로 불필요

## Non-Goals

- spark-submit 실행 검증 (사용자가 직접 진행)
- `kafka_to_iceberg_batch.py` 포팅 (이미 상위 호환 존재)
- `watermark_maintenance.py` 흡수 (별도 유지)
- Polaris 카탈로그 DAG 작성 (Glue 기준으로 포팅, 필요 시 추후 추가)

## Acceptance Criteria

- [ ] 6개 파이프라인 각각 DAG + src + yml 3파일 작성 완료
- [ ] 기존 workflow DAG 패턴 준수 (SparkSubmitOperator, Slack notifier, OpenLineage, `name=DAG_ID`)
- [ ] `Settings`에 raw 저장 경로 속성 추가
- [ ] Parquet 파이프라인이 `s3://<bucket>/data/raw/<schema>/<table>` 경로로 저장하도록 구현
- [ ] schema_sync가 5개 비교 항목을 모두 수행하고, 미반영 컬럼을 리포팅
- [ ] schema_sync의 주석 동기화가 `ALTER TABLE` 메타데이터 작업으로 구현 (데이터 스냅샷 미생성)
- [ ] iceberg_maintenance가 YAML 지정 스키마/테이블 대상으로 3단계(compaction→expire→orphan) 순차 실행
- [ ] 기존 `watermark_maintenance` DAG/src와 독립적으로 동작
- [ ] 코드 품질이 기존 workflow src 파일(kafka_to_iceberg.py 등) 수준과 동일

## Assumptions Exposed & Resolved

| Assumption                                    | Challenge                                                | Resolution                                     |
| --------------------------------------------- | -------------------------------------------------------- | ---------------------------------------------- |
| 모든 7개 기능을 포팅해야 한다                 | kafka_to_iceberg_batch는 이미 상위 호환                  | 3번 제외, 6개만 포팅                           |
| \*\_to_parquet이 불필요할 수 있다             | Iceberg 직접 적재가 있으니 Parquet 중간 단계가 필요한가? | 백업/대체 적재 포맷으로 유효                   |
| schema_to_iceberg의 빈 테이블 생성이 필요하다 | RTAS가 이미 자동 생성하는데 왜 필요한가?                 | 스키마 비교 + 주석 동기화로 기능 재정의        |
| maintenance를 배치 후속으로 연결해야 한다     | 독립 DAG vs 후속 태스크 vs 스키마별 분리                 | 일 1회 독립 DAG, 하나의 YAML로 관리            |
| watermark_maintenance를 범용에 흡수해야 한다  | 메타 테이블과 데이터 테이블 관리가 같은가?               | 별도 유지 (관심사 분리)                        |
| 삭제된 컬럼을 감지해야 한다                   | CDC가 컬럼 불일치를 자연 감지하지 않나?                  | 제외. 대신 미반영 컬럼 리포팅으로 대체         |
| 경로를 CLI args로 전달할 수 있다              | watermark_maintenance는 Settings 없이 args 사용          | Parquet 경로는 Settings 속성으로 추가 (일관성) |

## Technical Context

### 원본 프로젝트 (datalake-iceberg/src) 주요 패턴

- `Settings`는 Pydantic BaseSettings + YAML + Vault
- DB 스키마 조회: `database.py`의 `get_table_schema()`, `get_primary_key()`, `get_partition_key()`
- Iceberg 테이블 생성: `writeTo().using("iceberg").tableProperty(...)` RTAS 패턴
- Maintenance: Python `datetime` 사용하여 timestamp 계산 (SQL expression 불가)
- Parquet 저장: `df.write.mode("overwrite").parquet(path)` + datetime rebase 설정

### 대상 프로젝트 (workflow) 기존 구조

```
workflow/
├── dags/
│   ├── spark/           # DAG 파일
│   │   ├── glue_kafka_to_iceberg.py
│   │   ├── glue_mysql_to_iceberg.py
│   │   ├── glue_sqlserver_to_iceberg.py
│   │   └── glue_watermark_maintenance.py
│   ├── configs/         # YAML 설정
│   │   ├── glue_kafka_to_iceberg.yml
│   │   ├── glue_mysql_to_iceberg.yml
│   │   ├── glue_sqlserver_to_iceberg.yml
│   │   └── glue_watermark_maintenance.yml
│   └── dbt/             # dbt DAG
├── src/
│   ├── kafka_to_iceberg.py
│   ├── mysql_to_iceberg.py
│   ├── sqlserver_to_iceberg.py
│   ├── watermark_maintenance.py
│   └── utils/
│       ├── settings.py
│       ├── database.py
│       ├── listener.py
│       ├── signal.py
│       └── spark_logging.py
└── plugins/
    ├── operators/
    └── alerts/
```

### Settings 확장 필요

- 현재 `WAREHOUSE` property: `s3a://{bucket}/{iceberg_path}`
- 추가 필요: raw Parquet 저장 경로 property (예: `s3a://{bucket}/data/raw`)
- `AwsSettings`에 `raw_path` 또는 유사 필드 추가

### utils/database.py 재사용

- `BaseDatabaseManager.get_primary_key()`, `get_partition_key()`, `get_jdbc_options()` — Parquet 파이프라인에서 재사용
- schema_sync를 위해 `get_table_schema()`, `get_column_comments()` 등 확장 필요 가능성

## Ontology (Key Entities)

| Entity                | Type          | Fields                                                                     | Relationships                                                 |
| --------------------- | ------------- | -------------------------------------------------------------------------- | ------------------------------------------------------------- |
| schema_sync           | pipeline      | column_compare, comment_sync, type_mapping_rules, nullable_check, pk_check | reads source DB schema, compares with Iceberg, syncs comments |
| kafka_to_s3           | pipeline      | kafka_topic, avro_schema, parquet, s3_raw_path                             | reads from Kafka, writes Parquet to S3 raw                    |
| mssql_to_parquet      | pipeline      | jdbc, mssql_table, parquet, s3_raw_path                                    | reads from SQL Server, writes Parquet to S3 raw               |
| mysql_to_parquet      | pipeline      | jdbc, mysql_table, parquet, s3_raw_path                                    | reads from MySQL, writes Parquet to S3 raw                    |
| parquet_to_iceberg    | pipeline      | s3_parquet_path, iceberg_table, timestamp_normalization                    | converts S3 Parquet to Iceberg                                |
| iceberg_maintenance   | pipeline      | compaction, snapshot_expire, orphan_cleanup, target_tables_yaml            | maintains data Iceberg tables daily                           |
| watermark_maintenance | pipeline      | retention_days, cdc_watermark                                              | maintains meta table separately (existing, unchanged)         |
| DAG                   | orchestration | schedule, operator, config_yaml                                            | orchestrates Spark apps via spark-submit                      |
| Spark App             | compute       | src_file, args, SparkSession                                               | executed by DAG                                               |
| YAML Config           | configuration | job_settings, table_list, s3_path                                          | consumed by DAG                                               |
| type_mapping_rules    | supporting    | source_type, spark_type, iceberg_type                                      | used by schema_sync                                           |

## Ontology Convergence

| Round | Entity Count | New | Changed | Stable | Stability Ratio |
| ----- | ------------ | --- | ------- | ------ | --------------- |
| 1     | 9            | 9   | -       | -      | -               |
| 2     | 9            | 0   | 0       | 9      | 100%            |
| 3     | 9            | 0   | 0       | 9      | 100%            |
| 4     | 9            | 0   | 0       | 9      | 100%            |
| 5     | 10           | 1   | 0       | 9      | 90%             |
| 6     | 10           | 0   | 0       | 10     | 100%            |
| 7     | 11           | 2   | 1       | 8      | 82%             |
| 8     | 11           | 0   | 1       | 10     | 91%             |
| 9     | 11           | 0   | 0       | 11     | 100%            |

## Interview Transcript

<details>
<summary>Full Q&A (9 rounds)</summary>

### Round 1

**Q:** 위 7개 중 이번에 포팅할 대상은 전부인가요, 아니면 특정 항목만 선별하실 건가요?
**A:** 1, 2, 4, 5, 6, 7번. 3번은 지금 방식이 훨씬 더 좋은 구조의 상위 호환 같아.
**Ambiguity:** 69% (Goal: 0.5, Constraints: 0.2, Criteria: 0.1, Context: 0.4)

### Round 2

**Q:** Parquet 중간 적재가 필요한 구체적인 사용 시나리오가 있나요?
**A:** 데이터의 적재는 어떤 형태로 이뤄질지 모르기 때문에, iceberg 구조가 아니라 단순 parquet 백업일 수도 있지.
**Ambiguity:** 63.5% (Goal: 0.55, Constraints: 0.35, Criteria: 0.1, Context: 0.4)

### Round 3

**Q:** 포팅 완료의 기준이 코드 작성까지인지, spark-submit 실행 검증까지인지?
**A:** workflow에 적용되어 있는 코드품질 기준으로 기존 패턴(DAG + src + yml)에 맞춰 작성하는거야. spark-submit 실행 검증은 내가 직접 진행하려고.
**Ambiguity:** 49% (Goal: 0.6, Constraints: 0.4, Criteria: 0.55, Context: 0.45)

### Round 4

**Q:** maintenance를 범용(전체 테이블)으로 할지, yml 지정 방식으로 할지?
**A:** 고민됨. 스키마/테이블 많아서 효과적 유지보수 원함. Best practice 가이드 요청. (가이드 제공 후 후속 결정)
**Ambiguity:** 49% (Goal: 0.6, Constraints: 0.4, Criteria: 0.55, Context: 0.45)

### Round 5

**Q:** maintenance 구조 선택 + watermark_maintenance 흡수 여부
**A:** 일 1회 독립 DAG. YAML에 대상 스키마/테이블 지정. compaction+expire+orphan 모두 매일. watermark는 별도 유지 (메타 vs 데이터 분리).
**Ambiguity:** 34.5% (Goal: 0.75, Constraints: 0.6, Criteria: 0.6, Context: 0.5)

### Round 6

**Q:** Parquet 저장 경로 구조 — 동일 warehouse 하위? 별도 S3 경로?
**A:** s3://<버킷>/data/raw/<schema>/<table> 경로로 저장해야해.
**Ambiguity:** 30.5% (Goal: 0.75, Constraints: 0.7, Criteria: 0.6, Context: 0.55)

### Round 7 (Simplifier Mode)

**Q:** schema_to_iceberg가 필요한 시나리오 — 데이터 적재 전 테이블 생성? 언제 실행?
**A:** 용도 거의 없음. 기능 변경: 원천 vs Iceberg 스키마 비교 + 주석 동기화. 비교 항목: (1)컬럼 수/순서/타입, (2)주석 동기화. 추가 비교 항목 가이드 요청.
**Ambiguity:** 27.5% (Goal: 0.85, Constraints: 0.7, Criteria: 0.65, Context: 0.6)

### Round 8

**Q:** schema 비교 최종 항목 선택 (7개 중 채택/제외)
**A:** 1,2,3,4,5 채택. 7(삭제된 컬럼)은 CDC 자연 오류로 불필요, 대신 1번 확장으로 미반영 컬럼 리포팅. 6(신규 테이블) 제외.
**Ambiguity:** 22.5% (Goal: 0.9, Constraints: 0.75, Criteria: 0.7, Context: 0.65)

### Round 9

**Q:** 버킷/경로 주입 방식 (Settings vs args vs YAML)
**A:** Settings에 새 속성 추가 방식. 이름은 적절히 선정 위임.
**Ambiguity:** 19.2% (Goal: 0.9, Constraints: 0.85, Criteria: 0.7, Context: 0.7)

</details>
