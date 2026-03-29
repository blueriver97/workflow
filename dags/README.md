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

각 DAG는 `configs/{DAG_ID}.yml` 파일에서 설정을 로드합니다.

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
