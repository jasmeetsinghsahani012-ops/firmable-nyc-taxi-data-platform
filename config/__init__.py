"""Configuration package for the data engineering platform."""

from config.settings import (
    DATA_DIR,
    LOOKUP_DATA_DIR,
    LOG_DIR,
    PROJECT_ROOT,
    RAW_DATA_DIR,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_CONFIG,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ROLE,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_USER,
    SNOWFLAKE_WAREHOUSE,
    SnowflakeConfig,
)

__all__ = [
    "DATA_DIR",
    "LOOKUP_DATA_DIR",
    "LOG_DIR",
    "PROJECT_ROOT",
    "RAW_DATA_DIR",
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_CONFIG",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_SCHEMA",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_WAREHOUSE",
    "SnowflakeConfig",
]
