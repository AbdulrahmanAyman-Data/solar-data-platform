from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date, hour
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def create_spark_session():
    return SparkSession.builder \
        .appName("SilverWeatherTransform") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://solar-minio:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    
    streaming_path = "s3a://bronze/realtime/weather"
    historical_path = "s3a://bronze/historical/weather"
    silver_path = "s3a://silver/weather"

    # 1. Read Bronze sources conditionally
    dfs = []
    
    try:
        streaming_df = spark.read.format("delta").load(streaming_path)
        dfs.append(streaming_df)
        print(f"Successfully loaded streaming Bronze data from {streaming_path}")
    except Exception as e:
        print(f"Skipping streaming Bronze source (not found yet): {e}")

    try:
        historical_df = spark.read.format("delta").load(historical_path)
        dfs.append(historical_df)
        print(f"Successfully loaded historical Bronze data from {historical_path}")
    except Exception as e:
        print(f"Skipping historical Bronze source (not found yet): {e}")

    if not dfs:
        raise ValueError("No Bronze sources found to process! Aborting Silver transformation.")

    # 2. Union all available sources
    combined_df = dfs[0]
    for next_df in dfs[1:]:
        combined_df = combined_df.unionByName(next_df, allowMissingColumns=True)

    # 3. Apply physical range validations (allowing NULLs)
    validated_df = combined_df.filter(
        (col("latitude").isNull() | ((col("latitude") >= -90) & (col("latitude") <= 90))) &
        (col("longitude").isNull() | ((col("longitude") >= -180) & (col("longitude") <= 180))) &
        (col("temperature_c").isNull() | ((col("temperature_c") >= -90) & (col("temperature_c") <= 60))) &
        (col("humidity_pct").isNull() | ((col("humidity_pct") >= 0) & (col("humidity_pct") <= 100))) &
        (col("cloud_cover_pct").isNull() | ((col("cloud_cover_pct") >= 0) & (col("cloud_cover_pct") <= 100))) &
        (col("ghi_w_m2").isNull() | ((col("ghi_w_m2") >= 0) & (col("ghi_w_m2") <= 1500))) &
        (col("dni_w_m2").isNull() | ((col("dni_w_m2") >= 0) & (col("dni_w_m2") <= 1500)))
    )

    # 4. Full-table deduplication
    window_spec = Window.partitionBy("source", "location_id", "observation_timestamp") \
                        .orderBy(col("ingestion_timestamp").desc())
                        
    deduped_df = validated_df.with_row_number(window_spec) if hasattr(validated_df, "with_row_number") else \
                 validated_df.withColumn("row_num", row_number().over(window_spec))
                 
    deduped_df = deduped_df.filter(col("row_num") == 1).drop("row_num")

    # 5. Derive date and hour columns, clean up metadata columns
    transformed_df = deduped_df \
        .withColumn("observation_date", to_date(col("observation_timestamp"))) \
        .withColumn("observation_hour", hour(col("observation_timestamp"))) \
        .drop("kafka_timestamp") \
        .withColumn("silver_processed_timestamp", current_timestamp())

    # 6. Write out full overwrite to the Silver Delta table
    transformed_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(silver_path)

    print(f"Silver transformation completed successfully. Written to {silver_path}")

    spark.stop()

if __name__ == "__main__":
    main()
