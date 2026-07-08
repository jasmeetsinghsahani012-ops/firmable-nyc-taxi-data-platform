from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List

from config.settings import RAW_DATA_DIR, LOOKUP_DATA_DIR

logger = logging.getLogger(__name__)


class FileValidationError(Exception):
    """Raised when required source files are missing or invalid."""


def validate_source_files() -> Dict[str, object]:
    """
    Validate local NYC TLC source files before loading to Snowflake.

    Checks:
    - Exactly 12 Yellow Taxi 2023 parquet files exist.
    - Zone lookup CSV exists.
    - No parquet file is empty.
    """

    parquet_files: List[Path] = sorted(
        RAW_DATA_DIR.glob("yellow_tripdata_2023-*.parquet")
    )

    lookup_file = LOOKUP_DATA_DIR / "taxi_zone_lookup.csv"

    errors = []

    if len(parquet_files) != 12:
        errors.append(
            f"Expected 12 Yellow Taxi 2023 parquet files, found {len(parquet_files)}."
        )

    if not lookup_file.exists():
        errors.append(f"Missing lookup file: {lookup_file}")

    empty_files = [str(file) for file in parquet_files if file.stat().st_size == 0]

    if empty_files:
        errors.append(f"Empty parquet files found: {empty_files}")

    if errors:
        for error in errors:
            logger.error(error)
        raise FileValidationError("Source file validation failed.")

    summary = {
        "parquet_file_count": len(parquet_files),
        "lookup_file_exists": lookup_file.exists(),
        "total_parquet_size_mb": round(
            sum(file.stat().st_size for file in parquet_files) / (1024 * 1024), 2
        ),
        "files": [file.name for file in parquet_files],
    }

    logger.info("Source file validation passed: %s", summary)
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

    result = validate_source_files()

    print("Validation successful.")
    print(result)