import os

from pyspark.sql import SparkSession

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://solar-minio:9000")
MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123")
HIVE_METASTORE_URI = os.environ.get("HIVE_METASTORE_URI", "thrift://solar-hive-metastore:9083")


def build_spark_session(app_name: str, with_hive_metastore: bool = False) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ROOT_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_ROOT_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )
    if with_hive_metastore:
        builder = (
            builder.config("spark.hadoop.hive.metastore.uris", HIVE_METASTORE_URI)
            .config("spark.sql.catalogImplementation", "hive")
            .config("spark.sql.warehouse.dir", "s3a://warehouse/")
            .enableHiveSupport()
        )
    return builder.getOrCreate()
