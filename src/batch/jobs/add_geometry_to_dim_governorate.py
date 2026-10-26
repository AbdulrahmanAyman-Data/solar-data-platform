import pandas as pd
from pyspark.sql.functions import current_timestamp
from common.spark_session import build_spark_session 

GOLD_PATH = "s3a://gold/dim_governorate"
BOUNDARIES_PATH = "/opt/spark-notebooks/gis/data/processed/governorate_boundaries.parquet"

def main():
    spark = build_spark_session(app_name="solar-dim-governorate-geometry", with_hive_metastore=False)

    dim_df = spark.read.format("delta").load(GOLD_PATH).drop("gold_processed_timestamp")

    pdf = pd.read_parquet(BOUNDARIES_PATH)
    geom_df = spark.createDataFrame(pdf)

    before_count = dim_df.count()
    merged = (
        dim_df.join(geom_df, on="location_id", how="left")
        .withColumn("gold_processed_timestamp", current_timestamp())
    )
    after_count = merged.count()
    assert before_count == after_count == 27, f"العدد اتغيّر: {before_count} -> {after_count}"

    missing = merged.filter(merged.geometry_wkt.isNull()).select("location_id").collect()
    if missing:
        print("⚠️ محافظات من غير geometry:", [r.location_id for r in missing])

    (
        merged.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_PATH)
    )
    print(f"[add_geometry_to_dim_governorate] wrote {after_count} row(s), schema updated")
    spark.stop()

if __name__ == "__main__":
    main()