from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType


def build_schema() -> StructType:
    return StructType(
        [
            StructField("c1", IntegerType(), True),
            StructField("c2", IntegerType(), True),
        ]
    )


def build_session() -> SparkSession:

    threads = 2
    memory = "4g"

    return (
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
        .getOrCreate()
    )
