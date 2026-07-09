from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark_session() -> SparkSession:
    """
    Build Spark session for large-scale NYC TLC historical processing.

    In production this can run on AWS EMR, AWS Glue, Databricks, or Kubernetes.
    """
    return (
        SparkSession.builder
        .appName("nyc-tlc-historical-daily-aggregation")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def process_historical_trips(input_path: str, output_path: str) -> None:
    """
    Process full historical NYC Yellow Taxi data and create daily aggregates.

    Expected input:
        s3://nyc-tlc/trip data/yellow_tripdata_*.parquet

    Output:
        Daily zone-level aggregation partitioned by pickup_year and pickup_month.

    Optimization choices:
    - Read Parquet directly to benefit from columnar storage and predicate pushdown.
    - Select only required columns to reduce scan and shuffle cost.
    - Filter invalid records early to reduce downstream processing volume.
    - Aggregate at daily + pickup zone grain to avoid retaining trip-level history.
    - Partition output by year/month for efficient downstream pruning.
    """

    spark = build_spark_session()

    trips = (
        spark.read.parquet(input_path)
        .select(
            F.col("tpep_pickup_datetime"),
            F.col("tpep_dropoff_datetime"),
            F.col("PULocationID").alias("pickup_location_id"),
            F.col("DOLocationID").alias("dropoff_location_id"),
            F.col("passenger_count"),
            F.col("trip_distance"),
            F.col("fare_amount"),
            F.col("tip_amount"),
            F.col("total_amount"),
            F.col("payment_type"),
        )
    )

    cleaned = (
        trips
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_year", F.year("tpep_pickup_datetime"))
        .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
        .withColumn(
            "trip_duration_minutes",
            (
                F.unix_timestamp("tpep_dropoff_datetime")
                - F.unix_timestamp("tpep_pickup_datetime")
            ) / 60,
        )
        .filter(F.col("pickup_date").isNotNull())
        .filter(F.col("pickup_location_id").isNotNull())
        .filter(F.col("total_amount") > 0)
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("trip_duration_minutes") > 0)
    )

    daily_agg = (
        cleaned
        .groupBy(
            "pickup_year",
            "pickup_month",
            "pickup_date",
            "pickup_location_id",
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("fare_amount").alias("avg_fare_amount"),
            F.avg("tip_amount").alias("avg_tip_amount"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("trip_duration_minutes").alias("avg_trip_duration_minutes"),
        )
    )

    (
        daily_agg
        .repartition("pickup_year", "pickup_month")
        .write
        .mode("overwrite")
        .partitionBy("pickup_year", "pickup_month")
        .parquet(output_path)
    )

    spark.stop()


if __name__ == "__main__":
    INPUT_PATH = "s3://nyc-tlc/trip data/yellow_tripdata_*.parquet"
    OUTPUT_PATH = "s3://your-bucket/nyc-tlc/daily_zone_aggregates/"

    process_historical_trips(INPUT_PATH, OUTPUT_PATH)