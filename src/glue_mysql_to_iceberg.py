import pyspark.sql.functions as F
import pyspark.sql.types as T  # noqa
from pyspark.sql import Column, DataFrame, SparkSession  # noqa

from utils.database import get_jdbc_options, get_partition_key_info, get_primary_keys

# --- Import common modules ---
from utils.logging import SparkLoggerManager
from utils.settings import Settings


def process_single_table(
    spark: SparkSession,
    config: Settings,
    table_name: str,
    primary_keys: dict[str, list[str]],
    partition_keys: dict[str, str],
    jdbc_options: dict,
) -> None:
    """
    Process a single table from MySQL and save it as an Iceberg table.

    :param spark: SparkSession instance for data processing
    :param config: Settings object containing configuration parameters
    :param table_name: Name of the table to process in schema.table format
    :return: None
    """
    logger = SparkLoggerManager().get_logger()

    schema, table = table_name.split(".")
    bronze_schema = f"{schema.lower()}_bronze"
    target_table = table.lower()
    full_table_name = f"{config.CATALOG}.{bronze_schema}.{target_table}"

    pk_cols = primary_keys.get(table_name, [])
    partition_column = partition_keys.get(table_name)

    # Read from MySQL with or without partitioning
    if partition_column:
        logger.info(f"Reading '{table_name}' with partitioning on column '{partition_column}'.")
        bound_query = f"SELECT min({partition_column}) as `lower`, max({partition_column}) as `upper` FROM {table_name}"
        bound_df = spark.read.format("jdbc").options(**jdbc_options).option("query", bound_query).load()
        bounds = bound_df.first()

        if not bounds or bounds["lower"] is None:
            logger.warn(
                f"Partition column '{partition_column}' has no data for table '{table_name}'. Reading without partitioning."
            )
            jdbc_df = spark.read.format("jdbc").options(**jdbc_options).option("dbtable", table_name).load()
        else:
            jdbc_df = (
                spark.read.format("jdbc")
                .options(**jdbc_options)
                .option("dbtable", table_name)
                .option("partitionColumn", partition_column)
                .option("lowerBound", bounds["lower"])
                .option("upperBound", bounds["upper"])
                .option("numPartitions", config.NUM_PARTITIONS)
                .load()
            )
    else:
        logger.info(f"Reading '{table_name}' without partitioning.")
        jdbc_df = spark.read.format("jdbc").options(**jdbc_options).option("dbtable", table_name).load()

    # Add last_applied_date column
    jdbc_df = jdbc_df.withColumn("last_applied_date", F.current_timestamp())

    # Create database if not exists
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.CATALOG}.{bronze_schema}")

    # Write to Iceberg
    # Note. Merge On Read / Accept-Schema 활성화 시 아래 옵션 추가 필요.
    # Note. write.spark.accept-any-schema 활성화 시 Merge Into ... UNRESOLVED_COLUMN.WITH_SUGGESTION 오류 발생됨.
    """
        'write.spark.accept-any-schema'='true',
        'write.delete.mode'='merge-on-read',
        'write.update.mode'='merge-on-read',
        'write.merge.mode'='merge-on-read',
    """
    if pk_cols:
        jdbc_df = jdbc_df.withColumn("id_iceberg", F.md5(F.concat_ws("|", *[F.col(c) for c in pk_cols])))

        (
            jdbc_df.limit(1)
            .writeTo(f"{full_table_name}")
            .using("iceberg")
            .tableProperty("location", f"{config.ICEBERG_S3_ROOT_PATH}/{bronze_schema}/{target_table}")
            .tableProperty("format-version", "2")
            .tableProperty("write.metadata.delete-after-commit.enabled", "true")
            .tableProperty("write.metadata.previous-versions-max", "5")
            .tableProperty("history.expire.max-snapshot-age-ms", "86400000")
            # .partitionedBy(F.bucket(num_partition, "id_iceberg"))
            .createOrReplace()
        )

        (
            jdbc_df.writeTo(f"{full_table_name}")
            # .partitionedBy(F.bucket(num_partition, "id_iceberg"))
            .overwritePartitions()
        )
    else:
        (
            jdbc_df.writeTo(f"{full_table_name}")
            .using("iceberg")
            .tableProperty("location", f"{config.ICEBERG_S3_ROOT_PATH}/{bronze_schema}/{target_table}")
            .tableProperty("format-version", "2")
            .createOrReplace()
        )


def main(spark: SparkSession, config: Settings) -> None:
    """
    Reads data from a MySQL database and saves it as Iceberg tables.
    """
    logger = SparkLoggerManager().get_logger()
    logger.info("Starting Iceberg table creation from MySQL.")

    # Note. Retrieve primary key and partition key information from the database.
    primary_keys: dict[str, list[str]] = get_primary_keys(spark, config)
    partition_keys: dict[str, str] = get_partition_key_info(spark, config)
    jdbc_options = get_jdbc_options(config)

    failed_tables = []
    success_count = 0

    for table_name in config.TABLE_LIST:
        try:
            process_single_table(spark, config, table_name, primary_keys, partition_keys, jdbc_options)
            success_count += 1
            progress = (success_count / len(config.TABLE_LIST)) * 100
            logger.info(f"[{progress:3.1f}%] Successfully created or replaced {table_name}")
        except Exception as e:
            failed_tables.append({"table": table_name, "error": str(e)})
            logger.error(f"[FAIL] Skipping table {table_name} due to error.")

    logger.info("--- Migration Process Summary ---")
    logger.info(f"Total: {len(config.TABLE_LIST)}, Success: {success_count}, Fail: {len(failed_tables)}")

    if failed_tables:
        for fail in failed_tables:
            logger.warn(f"Failed Table: {fail['table']} | Reason: {fail['error']}")

        raise RuntimeError(f"Process finished with {len(failed_tables)} failures.")

    logger.info("Iceberg table creation process finished successfully.")


if __name__ == "__main__":
    settings = Settings()
    settings.init_vault()

    spark = (
        SparkSession.builder.appName("mysql_to_iceberg")
        .config("spark.sql.defaultCatalog", settings.CATALOG)
        .config(f"spark.sql.catalog.{settings.CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{settings.CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{settings.CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{settings.CATALOG}.warehouse", settings.ICEBERG_S3_ROOT_PATH)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
        )
        .config("spark.sql.caseSensitive", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.SimplifyCasts")
        .getOrCreate()
    )

    log_manager = SparkLoggerManager()
    log_manager.setup(spark)

    main(spark, settings)
    spark.stop()
