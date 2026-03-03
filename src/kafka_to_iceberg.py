import json
import threading
from argparse import ArgumentParser
from textwrap import dedent

import pyspark.sql.functions as F
import pyspark.sql.types as T
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql.avro.functions import from_avro

# --- Import common modules ---
from utils.settings import Settings
from utils.spark_logging import SparkLoggerManager


def extract_debezium_schema(schema: dict) -> dict:
    """Debezium 메시지 스키마에서 컬럼 이름과 Debezium 커넥터 타입을 추출합니다."""
    debezium_type = {}

    # 1. Envelope 스키마의 'fields' 목록에서 'before' 또는 'after' 필드를 찾습니다.
    envelope_fields = schema.get("fields", [])
    value_schema = None
    for field in envelope_fields:
        if field.get("name") in ("before", "after"):
            # 타입 정보는 ['null', {실제 스키마}] 형태일 수 있습니다.
            type_definitions = field.get("type", [])
            for type_def in type_definitions:
                if isinstance(type_def, dict) and "fields" in type_def:
                    value_schema = type_def
                    break
            if value_schema:
                break

    if not value_schema:
        print("Error: 'before' 또는 'after' 필드에서 유효한 스키마를 찾을 수 없습니다.")
        return {}

    # 2. 찾은 스키마 내부의 'fields' (컬럼 목록)를 순회합니다.
    column_fields = value_schema.get("fields", [])
    for col_field in column_fields:
        col_name = col_field.get("name")
        if not col_name:
            continue

        type_info = col_field.get("type")

        # 3. 컬럼 타입을 처리합니다. (Nullable 여부 고려)
        actual_type_def = None
        if isinstance(type_info, list):
            # Nullable 타입: ['null', type] 형태에서 실제 타입 정보를 찾습니다.
            for item in type_info:
                if item != "null":
                    actual_type_def = item
                    break
        else:
            # Non-nullable 타입
            actual_type_def = type_info

        if not actual_type_def:
            continue

        # 4. 최종 커넥터 타입을 추출합니다.
        # 복합 타입(dict)인 경우 'connect.name'을 우선적으로 사용하고,
        # 없는 경우 'type'을 사용합니다.
        # 단순 타입(str)인 경우 해당 문자열을 그대로 사용합니다.
        dbz_connector_type = None
        if isinstance(actual_type_def, dict):
            dbz_connector_type = actual_type_def.get("connect.name")
            if not dbz_connector_type:
                dbz_connector_type = actual_type_def.get("type")
        elif isinstance(actual_type_def, str):
            dbz_connector_type = actual_type_def

        if dbz_connector_type:
            debezium_type[col_name] = dbz_connector_type

    return debezium_type


def cast_column(column: Column, debezium_dtype: str) -> Column:
    """Debezium CDC 메시지의 데이터 타입을 Spark/Iceberg 타입으로 변환합니다.

    Debezium은 MySQL의 날짜/시간 타입을 다음과 같이 변환합니다:
    1. DATE 타입: 1970-01-01로부터의 일수 (정수)
       예: "date1": {"int": 18641} -> 1970-01-01부터 18641일 후

    2. TIME 타입: 자정으로부터의 마이크로초 (long)
       예: "time1": {"long": 18291000000} -> 00:00:00으로부터 18291000000 마이크로초 후

    3. DATETIME 타입: Unix epoch부터의 밀리초 (long)
       예: "datetime1": {"long": 1758712669000} -> 1970-01-01 00:00:00 UTC부터 1758712669000 밀리초 후

    4. DATETIME(6) 타입: Unix epoch부터의 마이크로초 (long)
       예: "create_datetime": {"long": 1758712669557813} -> 1970-01-01 00:00:00 UTC부터 1758712669557813 마이크로초 후

    5. TIMESTAMP 타입: ISO-8601 형식의 문자열
       예: "update_timestamp": {"string": "2025-09-24T02:17:49.557813Z"}

    Args:
        column: 변환할 Spark Column 객체
        debezium_dtype: Debezium 커넥터가 사용하는 데이터 타입 문자열

    Returns:
        변환된 Spark Column 객체
    """
    # Avro 스키마의 default: 0 때문에 강제 주입된 값을 걸러냅니다.
    # column.isNotNull() 만으로는 부족하며, 반드시 (column != 0) 체크가 병행되어야 합니다.

    if debezium_dtype == "io.debezium.time.Date":
        return F.date_add(F.lit("1970-01-01"), column.cast("int"))
    elif debezium_dtype == "io.debezium.time.MicroTime":
        return F.to_utc_timestamp(F.timestamp_seconds(column / 1_000_000), "UTC")
    elif debezium_dtype in ("io.debezium.time.Timestamp", "io.debezium.time.MicroTimestamp"):
        is_valid = column.isNotNull() & (column != 0)
        return F.when(is_valid, F.to_utc_timestamp(F.timestamp_millis(column), "Asia/Seoul")).otherwise(
            F.lit(None).cast(T.TimestampType())
        )  # 0을 NULL로 강제 환원
    elif debezium_dtype == "io.debezium.time.ZonedTimestamp":
        pass
    return column


def process_batch(
    batch_df: DataFrame,
    batch_id: int,
    spark: SparkSession,
    config: Settings,
    schema_registry_client: SchemaRegistryClient,
    topic: str,
) -> None:
    logger = SparkLoggerManager().get_logger()
    logger.info(f"<batch-{batch_id}, {batch_df.count()}> Processing {topic}")
    if batch_df.isEmpty():
        return

    batch_df.persist(StorageLevel.MEMORY_AND_DISK)

    # 1. 스키마 레지스트리에서 스키마 조회
    value_schema_ids = [row.value_schema_id for row in batch_df.select("value_schema_id").distinct().collect()]
    value_schema_dict = {sid: schema_registry_client.get_schema(sid).schema_str for sid in value_schema_ids}
    key_schema_ids = [row.key_schema_id for row in batch_df.select("key_schema_id").distinct().collect()]
    key_schema_dict = {sid: schema_registry_client.get_schema(sid).schema_str for sid in key_schema_ids}

    logger.info(f"Processing {topic} | Key Schema Ids: {key_schema_ids} | Value Schema Ids: {value_schema_ids}")

    # 2. 스키마 버전별 데이터 변환 및 Iceberg 적재
    for value_schema_id, value_schema_str in value_schema_dict.items():
        schema_filtered_df = batch_df.filter(F.col("value_schema_id") == value_schema_id)
        value_schema = json.loads(value_schema_str)
        debezium_schema = extract_debezium_schema(value_schema)

        current_key_schema_rows = schema_filtered_df.select("key_schema_id").distinct().collect()
        if not current_key_schema_rows:
            continue

        key_schema_id = current_key_schema_rows[0].key_schema_id
        key_schema_str = key_schema_dict.get(key_schema_id)
        if not key_schema_str:
            logger.warn(f"Key schema not found for id {key_schema_id}")
            continue

        key_schema = json.loads(key_schema_str)
        pk_cols = [field["name"] for field in key_schema["fields"]]

        # Avro 역직렬화 및 기본 변환
        transformed_df = (
            schema_filtered_df.withColumn("key", from_avro(F.col("key"), key_schema_str, {"mode": "FAILFAST"}))
            .withColumn("value", from_avro(F.col("value"), value_schema_str, {"mode": "FAILFAST"}))
            .withColumn(
                "id_iceberg",
                F.md5(F.concat_ws("|", *[cast_column(F.col(f"key.{c}"), debezium_schema.get(c, "")) for c in pk_cols])),
            )
            .select(
                "value.after.*",
                F.col("value.op").alias("__op"),
                F.col("offset").alias("__offset"),
                F.timestamp_millis(F.col("value.ts_ms")).alias("last_applied_date"),
                "id_iceberg",
            )
        )

        # 3. Iceberg 테이블 처리 (기존 process_table 로직 통합)
        prefix, schema, table = topic.split(".")
        iceberg_schema, iceberg_table = f"{schema.lower()}_bronze", table.lower()
        full_table_name = f"{config.CATALOG}.{iceberg_schema}.{iceberg_table}"
        try:
            catalog_schema = spark.table(full_table_name).schema
        except Exception:
            logger.warn(f"Table {full_table_name} not found. Skipping batch for this schema version.")
            continue

        # 최종 타입 캐스팅
        cdc_df = transformed_df.select(
            *[
                cast_column(F.col(f.name), debezium_schema.get(f.name, "")).cast(f.dataType).alias(f.name)
                for f in catalog_schema.fields
            ],
            "__op",
            "__offset",
        )

        # 중복 제거
        window_spec = Window.partitionBy("id_iceberg").orderBy(F.desc("__offset"))
        dedup_df = (
            cdc_df.withColumn("__row", F.row_number().over(window_spec))
            .filter(F.col("__row") == 1)
            .drop("__row", "__offset")
        )

        upsert_df = dedup_df.filter(F.col("__op") != "d").drop("__op")
        delete_df = dedup_df.filter(F.col("__op") == "d").drop("__op")

        # Upsert (Merge Into)
        if not upsert_df.isEmpty():
            upsert_df.createOrReplaceTempView(f"upsert_view_{table}")
            columns = upsert_df.columns
            update_expr = ", ".join([f"t.{c} = s.{c}" for c in columns])
            insert_cols = ", ".join(columns)
            insert_vals = ", ".join([f"s.{c}" for c in columns])
            merge_query = dedent(f"""
                MERGE INTO {full_table_name} t
                USING (SELECT * FROM upsert_view_{table}) s
                ON t.id_iceberg = s.id_iceberg
                WHEN MATCHED THEN UPDATE SET {update_expr}
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """)
            logger.info(f"Executing Merge Into for {full_table_name}")
            spark.sql(merge_query)

        # Delete
        if not delete_df.isEmpty():
            delete_df.createOrReplaceTempView(f"delete_view_{table}")
            delete_query = dedent(f"""
                DELETE FROM {full_table_name} t
                WHERE EXISTS (SELECT s.id_iceberg FROM delete_view_{table} s WHERE s.id_iceberg = t.id_iceberg)
            """)
            logger.info(f"Executing Delete for {full_table_name}")
            spark.sql(delete_query)

    batch_df.unpersist()


def run_topic_stream(spark: SparkSession, settings: Settings, topic: str):
    logger = SparkLoggerManager().get_logger()

    checkpoint_path = f"{settings.WAREHOUSE}/checkpoints/kafka_to_iceberg/{topic}"
    logger.info(f"Starting stream for topic: {topic}, checkpoint: {checkpoint_path}")

    if not settings.kafka:
        raise ValueError("Kafka configuration is missing. Please check your settings.")

    schema_registry_client = SchemaRegistryClient({"url": settings.kafka.schema_registry})
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", topic)
        .option("maxOffsetsPerTrigger", settings.kafka.max_offsets_per_trigger)
        .option("startingOffsets", settings.kafka.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    query = (
        kafka_df.withColumn("key_schema_id", F.expr("byte_to_int(substring(key, 2, 4))"))
        .withColumn("key", F.expr("substring(key, 6, length(key)-5)"))
        .withColumn("value_schema_id", F.expr("byte_to_int(substring(value, 2, 4))"))
        .withColumn("value", F.expr("substring(value, 6, length(value)-5)"))
        .selectExpr("key_schema_id", "value_schema_id", "key", "value", "topic", "offset")
        .writeStream.foreachBatch(
            lambda batch_df, batch_id: process_batch(batch_df, batch_id, spark, settings, schema_registry_client, topic)
        )
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--topics", type=str)
    args = parser.parse_args()
    settings = Settings()

    topics = args.topics.split(",")
    spark = (
        SparkSession.builder.appName("kafka_to_iceberg")
        .config("spark.sql.defaultCatalog", settings.CATALOG)
        .config(f"spark.sql.catalog.{settings.CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{settings.CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{settings.CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{settings.CATALOG}.warehouse", settings.WAREHOUSE)
        .config(f"spark.sql.catalog.{settings.CATALOG}.s3.path-style-access", "true")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
        )
        .config("spark.rdd.compress", "true")
        .config("spark.sql.caseSensitive", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.shuffle.service.removeShuffle", "true")
        # .config("spark.metrics.namespace", settings.kafka.metric_namespace)
        .getOrCreate()
    )

    # 로거 초기화
    logger_manager = SparkLoggerManager()
    logger_manager.setup(spark)

    # UDF 등록: Byte Array -> Integer (Schema ID 추출용)
    spark.udf.register("byte_to_int", lambda x: int.from_bytes(x, byteorder="big", signed=False))

    threads = []
    for topic in topics:
        t = threading.Thread(target=run_topic_stream, args=(spark, settings, topic))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    spark.stop()
