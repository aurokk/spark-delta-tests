from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)
from typing import Callable, Optional


def build_schema() -> StructType:
    return StructType(
        [
            StructField("id", LongType(), True),
            # 1
            StructField("field_0", StringType(), True),
            StructField("field_1", TimestampType(), True),
            StructField("field_2", IntegerType(), True),
            StructField("field_3", StringType(), True),
            StructField("field_4", TimestampType(), True),
            StructField("field_5", IntegerType(), True),
            StructField("field_6", StringType(), True),
            StructField("field_7", TimestampType(), True),
            StructField("field_8", IntegerType(), True),
            StructField("field_9", StringType(), True),
            # 11
            StructField("field_10", TimestampType(), True),
            StructField("field_11", IntegerType(), True),
            StructField("field_12", StringType(), True),
            StructField("field_13", TimestampType(), True),
            StructField("field_14", IntegerType(), True),
            StructField("field_15", StringType(), True),
            StructField("field_16", TimestampType(), True),
            StructField("field_17", IntegerType(), True),
            StructField("field_18", StringType(), True),
            StructField("field_19", TimestampType(), True),
            # 21
        ]
    )


def build_session(
    configure: Optional[Callable[[SparkSession.Builder], None]] = None,
) -> SparkSession:

    threads = 2
    memory = "4g"

    builder = (
        SparkSession.Builder()
        .appName("test_000")
        .master(f"local[{threads}]")
        .config("spark.driver.memory", memory)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.hadoop.fs.s3.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark. memory. fraction", 0.8)
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.ui.explainMode", "extended")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.hadoop.fs.s3a.access.key", "0c5UR65DCP6KrVhx6uBG")
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            "89d13tOEcq1nMgLPem6pF36FCbsS8SU5wx6StDmh",
        )
        .config(
            "spark.hadoop.fs.s3a.bucket.tests.endpoint",
            "http://minio:9000",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "10000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "1")
    )

    if configure is not None:
        configure(builder)

    return builder.getOrCreate()
