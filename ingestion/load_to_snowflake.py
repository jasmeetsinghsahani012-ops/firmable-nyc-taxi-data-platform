from __future__ import annotations

import logging
from pathlib import Path

from config.settings import RAW_DATA_DIR, LOOKUP_DATA_DIR
from ingestion.snowflake_client import SnowflakeClient
from ingestion.validate_files import validate_source_files

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


RAW_TRIPS_TABLE = "YELLOW_TAXI_TRIPS_2023"
ZONE_LOOKUP_TABLE = "TAXI_ZONE_LOOKUP"
PARQUET_STAGE = "YELLOW_TAXI_PARQUET_STAGE"
LOOKUP_STAGE = "TAXI_LOOKUP_STAGE"


def create_snowflake_objects(client: SnowflakeClient) -> None:
    """Create stages and raw tables required for ingestion."""

    client.execute(f"""
        CREATE OR REPLACE STAGE {PARQUET_STAGE}
        FILE_FORMAT = (TYPE = PARQUET)
    """)

    client.execute(f"""
        CREATE OR REPLACE STAGE {LOOKUP_STAGE}
        FILE_FORMAT = (
            TYPE = CSV
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        )
    """)

    client.execute(f"""
        CREATE OR REPLACE TABLE {RAW_TRIPS_TABLE} (
            vendor_id INTEGER,
            tpep_pickup_datetime TIMESTAMP_NTZ,
            tpep_dropoff_datetime TIMESTAMP_NTZ,
            passenger_count INTEGER,
            trip_distance FLOAT,
            ratecode_id INTEGER,
            store_and_fwd_flag STRING,
            pu_location_id INTEGER,
            do_location_id INTEGER,
            payment_type INTEGER,
            fare_amount FLOAT,
            extra FLOAT,
            mta_tax FLOAT,
            tip_amount FLOAT,
            tolls_amount FLOAT,
            improvement_surcharge FLOAT,
            total_amount FLOAT,
            congestion_surcharge FLOAT,
            airport_fee FLOAT,
            source_file STRING,
            loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)

    client.execute(f"""
        CREATE OR REPLACE TABLE {ZONE_LOOKUP_TABLE} (
            location_id INTEGER,
            borough STRING,
            zone STRING,
            service_zone STRING,
            loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)


def put_files_to_stage(client: SnowflakeClient) -> None:
    """Upload local source files to Snowflake internal stages."""

    parquet_path = str(RAW_DATA_DIR / "yellow_tripdata_2023-*.parquet").replace("\\", "/")
    lookup_path = str(LOOKUP_DATA_DIR / "taxi_zone_lookup.csv").replace("\\", "/")

    logger.info("Uploading parquet files to Snowflake stage.")
    client.execute(f"PUT file://{parquet_path} @{PARQUET_STAGE} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

    logger.info("Uploading zone lookup file to Snowflake stage.")
    client.execute(f"PUT file://{lookup_path} @{LOOKUP_STAGE} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")


def copy_into_tables(client: SnowflakeClient) -> None:
    """Load staged files into raw Snowflake tables."""

    logger.info("Loading parquet files into raw taxi trips table.")

    client.execute(f"""
        COPY INTO {RAW_TRIPS_TABLE} (
            vendor_id,
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            passenger_count,
            trip_distance,
            ratecode_id,
            store_and_fwd_flag,
            pu_location_id,
            do_location_id,
            payment_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            congestion_surcharge,
            airport_fee,
            source_file
        )
        FROM (
            SELECT
                $1:VendorID::INTEGER,
                $1:tpep_pickup_datetime::TIMESTAMP_NTZ,
                $1:tpep_dropoff_datetime::TIMESTAMP_NTZ,
                $1:passenger_count::INTEGER,
                $1:trip_distance::FLOAT,
                $1:RatecodeID::INTEGER,
                $1:store_and_fwd_flag::STRING,
                $1:PULocationID::INTEGER,
                $1:DOLocationID::INTEGER,
                $1:payment_type::INTEGER,
                $1:fare_amount::FLOAT,
                $1:extra::FLOAT,
                $1:mta_tax::FLOAT,
                $1:tip_amount::FLOAT,
                $1:tolls_amount::FLOAT,
                $1:improvement_surcharge::FLOAT,
                $1:total_amount::FLOAT,
                $1:congestion_surcharge::FLOAT,
                $1:Airport_fee::FLOAT,
                METADATA$FILENAME
            FROM @{PARQUET_STAGE}
        )
        FILE_FORMAT = (TYPE = PARQUET)
        ON_ERROR = ABORT_STATEMENT
    """)

    logger.info("Loading taxi zone lookup table.")

    client.execute(f"""
        COPY INTO {ZONE_LOOKUP_TABLE} (
            location_id,
            borough,
            zone,
            service_zone
        )
        FROM @{LOOKUP_STAGE}
        FILE_FORMAT = (
            TYPE = CSV
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        )
        ON_ERROR = ABORT_STATEMENT
    """)


def print_row_counts(client: SnowflakeClient) -> None:
    trips_count = client.fetch_dataframe(f"SELECT COUNT(*) AS row_count FROM {RAW_TRIPS_TABLE}")
    lookup_count = client.fetch_dataframe(f"SELECT COUNT(*) AS row_count FROM {ZONE_LOOKUP_TABLE}")

    logger.info("Raw taxi trips row count: %s", trips_count.iloc[0]["ROW_COUNT"])
    logger.info("Taxi zone lookup row count: %s", lookup_count.iloc[0]["ROW_COUNT"])


def main() -> None:
    logger.info("Starting NYC taxi Snowflake ingestion.")

    validate_source_files()

    with SnowflakeClient() as client:
        create_snowflake_objects(client)
        put_files_to_stage(client)
        copy_into_tables(client)
        print_row_counts(client)

    logger.info("Snowflake ingestion completed successfully.")


if __name__ == "__main__":
    main()