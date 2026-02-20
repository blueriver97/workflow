import json
from functools import lru_cache
from textwrap import dedent

import pyspark.sql.functions as F
import pyspark.sql.types as T
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql.avro.functions import from_avro

# --- Import common modules ---
from utils.logging import SparkLoggerManager
from utils.settings import Settings


# --- Schema Registry Caching ---
@lru_cache(maxsize=128)
def get_schema_from_registry(client: SchemaRegistryClient, schema_id: int) -> str:
    """
    Schema Registry에서 스키마를 조회하고 결과를 캐싱합니다.
    """
    return client.get_schema(schema_id).schema_str


def extract_debezium_schema(schema: dict) -> dict:
    """
    Debezium JSON 스키마에서 컬럼별 데이터 타입을 추출합니다.
    """
    debezium_type = {}
    envelope_fields = schema.get("fields", [])
    value_schema = None

    # before/after 필드 내 실제 데이터 스키마 검색
    for field in envelope_fields:
        if field.get("name") in ("before", "after"):
            type_definitions = field.get("type", [])  # 타입 정보는 ['null', {실제 스키마}] 형태일 수 있음.
            for type_def in type_definitions:
                if isinstance(type_def, dict) and "fields" in type_def:
                    value_schema = type_def
                    break
            if value_schema:
                break

    if not value_schema:
        return {}

    # 찾은 스키마 내부의 'fields' (컬럼 목록) 순회
    column_fields = value_schema.get("fields", [])
    for col_field in column_fields:
        col_name = col_field.get("name")
        if not col_name:
            continue

        type_info = col_field.get("type")
        actual_type_def = None

        # 컬럼 타입 처리 (Nullable 여부 고려)
        if isinstance(type_info, list):
            # Nullable 타입: ['null', type] 형태에서 실제 타입 정보를 참조
            for item in type_info:
                if item != "null":
                    actual_type_def = item
                    break
        else:
            # Non-nullable 타입
            actual_type_def = type_info

        if not actual_type_def:
            continue

        # 최종 커넥터 타입 추출
        # 복합 타입(dict)인 경우 'connect.name'을 우선적으로 사용하고, 없는 경우 'type' 사용
        # 단순 타입(str)인 경우 해당 문자열을 그대로 사용
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


def cast_dataframe(df: DataFrame, catalog_schema: T.StructType, debezium_dtypes: dict) -> DataFrame:
    """
    Debezium 타입을 Spark/Iceberg 호환 타입으로 캐스팅합니다.

    "DATE": {"int": 18641},
    "TIME": {"long": 18291000000},
    "DATETIME": {"long": 1758712669000},
    "DATETIME(6)": {"long": 1758712669557813},
    "TIMESTAMP(6)": {"string": "2025-09-24T02:17:49.557813Z"}
    """

    def cast_column_type(field: T.StructField, debezium_dtype: str) -> Column:
        # 날짜 및 시간 타입 처리 (Scalability/Accuracy 고려)
        if debezium_dtype == "io.debezium.time.Date":
            return F.date_add(F.lit("1970-01-01"), F.col(field.name).cast("int")).alias(field.name)
        elif debezium_dtype == "io.debezium.time.MicroTime":
            return F.to_utc_timestamp(F.timestamp_seconds(F.col(field.name) / 1_000_000), "UTC").alias(field.name)
        elif debezium_dtype == "io.debezium.time.Timestamp":
            return F.to_utc_timestamp(F.timestamp_millis(F.col(field.name)), "Asia/Seoul").alias(field.name)
        elif debezium_dtype == "io.debezium.time.MicroTimestamp":
            return F.to_utc_timestamp(F.timestamp_micros(F.col(field.name)), "Asia/Seoul").alias(field.name)
        elif debezium_dtype == "io.debezium.time.ZonedTimestamp":
            # ISO-8601 문자열을 Timestamp로 변환
            return F.to_timestamp(F.col(field.name)).alias(field.name)

        return F.col(field.name).cast(field.dataType).alias(field.name)

    return df.select(
        *[cast_column_type(field, debezium_dtypes.get(field.name, "")) for field in catalog_schema.fields],
        "__op",
        "__offset",
    )


def process_table(
    spark: SparkSession,
    config: Settings,
    schema: str,
    table: str,
    table_df: DataFrame,
    parsed_debezium_schema: dict,
    pk_cols: list,
) -> None:
    """
    개별 테이블별로 Iceberg Merge/Delete를 수행합니다.
    """
    logger = SparkLoggerManager().get_logger()

    iceberg_schema, iceberg_table = f"{schema.lower()}_bronze", table.lower()
    full_table_name = f"{config.CATALOG}.{iceberg_schema}.{iceberg_table}"

    # Iceberg 테이블 존재 여부 확인 및 스키마 로드
    target_table = spark.table(full_table_name)
    catalog_schema = target_table.schema

    # PK 기반 hash ID 생성 (Performance: 복합키 대응)
    cdc_df = table_df.withColumn("last_applied_date", F.col("__ts_ms")).withColumn(
        "id_iceberg", F.md5(F.concat_ws("|", *[F.col(column) for column in pk_cols]))
    )
    cdc_df = cast_dataframe(cdc_df, catalog_schema, parsed_debezium_schema)

    # 중복 제거: 동일 PK 내 가장 최신 오프셋만 선택
    window_spec = Window.partitionBy("id_iceberg").orderBy(F.desc("__offset"))
    cdc_df = (
        cdc_df.withColumn("__row", F.row_number().over(window_spec))
        .filter(F.col("__row") == 1)
        .drop("__row", "__offset")
    ).cache()

    upsert_df = cdc_df.filter(F.col("__op") != "d").drop("__op")
    delete_df = cdc_df.filter(F.col("__op") == "d").drop("__op")

    if not upsert_df.isEmpty():
        upsert_df.createOrReplaceGlobalTempView("upsert_view")
        columns = target_table.columns
        update_expr = ", ".join([f"t.{c} = s.{c}" for c in columns])
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join([f"s.{c}" for c in columns])

        query = dedent(f"""
            MERGE INTO {full_table_name} t
            USING (SELECT * FROM global_temp.upsert_view) s
            ON t.id_iceberg = s.id_iceberg
            WHEN MATCHED THEN UPDATE SET {update_expr}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """)
        logger.info(f"Executing Upsert Query for {full_table_name}")
        spark.sql(query)

    if not delete_df.isEmpty():
        delete_df.createOrReplaceGlobalTempView("delete_view")
        query = dedent(f"""
            DELETE FROM {full_table_name} t
            WHERE EXISTS (
                SELECT 1 FROM global_temp.delete_view s
                WHERE s.id_iceberg = t.id_iceberg
            )
        """)
        logger.info(f"Executing Delete Query for {full_table_name}")
        spark.sql(query)

    cdc_df.unpersist()


def process_batch(batch_df: DataFrame, batch_id: int, spark: SparkSession, config: Settings) -> None:
    """
    마이크로 배치 데이터를 필터링하고 테이블별 처리를 호출합니다.
    """
    logger = SparkLoggerManager().get_logger()
    logger.info(f"Processing batch {batch_id} with {batch_df.count()} rows")

    if batch_df.isEmpty():
        return

    batch_df.persist(StorageLevel.MEMORY_AND_DISK)
    registry_client = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY})

    for table_identifier in config.TABLE_LIST:
        schema, table = table_identifier.split(".")
        target_topic = config.TOPIC_DICT.get(table_identifier)
        filtered_df = batch_df.filter(F.col("topic") == target_topic)

        if filtered_df.isEmpty():
            continue

        # Schema ID 추출 (Performance: distinct() 이후 collect() 최적화)
        distinct_schemas = filtered_df.select("value_schema_id", "key_schema_id").distinct().collect()

        for row in distinct_schemas:
            v_sid, k_sid = row.value_schema_id, row.key_schema_id

            # 캐싱된 함수를 통한 스키마 로드
            value_schema_str = get_schema_from_registry(registry_client, v_sid)
            key_schema_str = get_schema_from_registry(registry_client, k_sid)

            pk_cols = [field["name"] for field in json.loads(key_schema_str)["fields"]]
            parsed_debezium_schema = extract_debezium_schema(json.loads(value_schema_str))

            # Avro 역직렬화 및 전처리
            transformed_df = (
                filtered_df.filter(F.col("value_schema_id") == v_sid)
                .withColumn(
                    "data",
                    from_avro(F.col("value"), value_schema_str, {"mode": "FAILFAST"}),
                )
                .select(
                    "data.after.*",
                    F.col("data.ts_ms").alias("__ts_ms"),
                    F.col("data.op").alias("__op"),
                    F.col("offset").alias("__offset"),
                )
            )

            process_table(spark, config, schema, table, transformed_df, parsed_debezium_schema, pk_cols)

    batch_df.unpersist()


if __name__ == "__main__":
    # 1. Load settings from .env file
    # dev.env 또는 prod.env 파일을 지정하여 환경을 선택할 수 있습니다.
    # settings = Settings(_env_file="src/dev.env", _env_file_encoding="utf-8")

    settings = Settings()

    spark = (
        SparkSession.builder.appName("kafka_to_iceberg_cdc")
        .config("spark.sql.defaultCatalog", settings.CATALOG)
        .config(f"spark.sql.catalog.{settings.CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{settings.CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{settings.CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{settings.CATALOG}.warehouse", settings.ICEBERG_S3_ROOT_PATH)
        .config(f"spark.sql.catalog.{settings.CATALOG}.s3.path-style-access", True)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
        )
        .config("spark.rdd.compress", True)
        .config("spark.sql.caseSensitive", True)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.shuffle.service.removeShuffle", True)
        .config("spark.metrics.namespace", settings.METRIC_NAMESPACE)
        .getOrCreate()
    )

    logger_manager = SparkLoggerManager()
    logger_manager.setup(spark)

    udf_byte2int = spark.udf.register("byte_to_int", lambda x: int.from_bytes(x, byteorder="big", signed=False))

    """
    startingOffsets
    - earliest  : offset 맨 앞부터 가져 오기
    - latest    : 마지막 offset 이후 신규 인입 메시지 가져 오기
    """
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.BOOTSTRAP_SERVERS)
        .option("subscribe", ",".join(settings.TOPIC_LIST))
        .option("maxOffsetsPerTrigger", settings.MAX_OFFSETS_PER_TRIGGER)
        .option("startingOffsets", settings.STARTING_OFFSETS)
        .option("failOnDataLoss", False)
        .load()
    )

    """
    Confluent Avro Wire Format 파싱 (Magic Byte + Schema ID + Payload)
    - 1     : MAGIC BYTE, 항상 \x00 값 사용
    - 2~5   : SCHEMA ID, b'\x00\x00\x00\x08'
    - 6~    : 실제 메시지 내용
    """
    query = (
        kafka_df.withColumn("key_schema_id", udf_byte2int(F.substring("key", 2, 4)))  # SCHEMA_ID = b'\x00\x00\x00\x08'
        .withColumn("key", F.expr("substring(key, 6, length(key)-5)"))
        .withColumn("value_schema_id", udf_byte2int(F.substring("value", 2, 4)))  # SCHEMA_ID = b'\x00\x00\x00\x08'
        .withColumn("value", F.expr("substring(value, 6, length(value)-5)"))
        .selectExpr("key_schema_id", "value_schema_id", "key", "value", "topic", "offset")
        .writeStream.foreachBatch(lambda batch_df, batch_id: process_batch(batch_df, batch_id, spark, settings))
        .option("checkpointLocation", settings.CHECKPOINT_LOCATION)
        .outputMode("append")
        .trigger(availableNow=True)
        # .trigger(processingTime="5 minutes")
        .start()
    )

    query.awaitTermination()
    spark.stop()
