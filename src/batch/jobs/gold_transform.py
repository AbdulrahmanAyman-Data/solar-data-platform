from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max

def main():
    spark = SparkSession.builder \
        .appName("GoldWeatherTransform") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://solar-minio:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    silver_df = spark.read.format("delta").load("s3a://silver/weather")

    # 1. Daily Radiation Aggregation
    silver_df.groupBy("location_id", "observation_date") \
        .agg(avg("ghi_w_m2").alias("avg_ghi"), max("ghi_w_m2").alias("max_ghi")) \
        .write.format("delta").mode("overwrite").save("s3a://gold/weather/solar_radiation_daily")

    # 2. Location Potential Aggregation
    silver_df.groupBy("location_id") \
        .agg(avg("ghi_w_m2").alias("overall_avg_ghi")) \
        .write.format("delta").mode("overwrite").save("s3a://gold/weather/solar_potential_by_location")

    # 3. Peak Hourly Production
    silver_df.groupBy("location_id", "observation_hour") \
        .agg(avg("ghi_w_m2").alias("avg_hourly_ghi")) \
        .write.format("delta").mode("overwrite").save("s3a://gold/weather/peak_production_hours")

    print("Gold transformations completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()
