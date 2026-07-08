"""Central configuration for the data engineering platform.

All secrets and environment-specific values are loaded from the project
``.env`` file via ``python-dotenv``. Nothing is hardcoded in source.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Project paths
# ---------------------------------------------------------------------------

# Resolve project root relative to this file: config/settings.py -> project root
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent

# Load environment variables from the project-level .env file.
# Existing process environment variables take precedence (override=False).
_ENV_FILE: Path = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=_ENV_FILE, override=False)

# Standard directory layout for local data and operational logs
DATA_DIR: Path = PROJECT_ROOT / "data"
RAW_DATA_DIR: Path = DATA_DIR / "raw"
LOOKUP_DATA_DIR: Path = DATA_DIR / "lookup"
LOG_DIR: Path = PROJECT_ROOT / "logs"

# ---------------------------------------------------------------------------
# Snowflake environment variable names
# ---------------------------------------------------------------------------

_SNOWFLAKE_ENV_KEYS: tuple[str, ...] = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
    "SNOWFLAKE_WAREHOUSE",
)


def _require_env(name: str) -> str:
    """Return a required environment variable or raise a clear error."""
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise EnvironmentError(
            f"Required environment variable '{name}' is missing or empty. "
            f"Define it in '{_ENV_FILE}' or export it in your shell."
        )
    return value.strip()


# ---------------------------------------------------------------------------
# Snowflake settings (loaded from .env)
# ---------------------------------------------------------------------------

SNOWFLAKE_ACCOUNT: str = _require_env("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER: str = _require_env("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD: str = _require_env("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ROLE: str = _require_env("SNOWFLAKE_ROLE")
SNOWFLAKE_DATABASE: str = _require_env("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA: str = _require_env("SNOWFLAKE_SCHEMA")
SNOWFLAKE_WAREHOUSE: str = _require_env("SNOWFLAKE_WAREHOUSE")


@dataclass(frozen=True, slots=True)
class SnowflakeConfig:
    """Immutable Snowflake connection configuration."""

    account: str
    user: str
    password: str
    role: str
    database: str
    schema: str
    warehouse: str

    @classmethod
    def from_env(cls) -> SnowflakeConfig:
        """Build configuration from currently loaded environment variables."""
        return cls(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            role=SNOWFLAKE_ROLE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            warehouse=SNOWFLAKE_WAREHOUSE,
        )

    def as_connection_params(self) -> dict[str, Any]:
        """Return keyword arguments for ``snowflake.connector.connect``."""
        return {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "role": self.role,
            "database": self.database,
            "schema": self.schema,
            "warehouse": self.warehouse,
        }

    def __repr__(self) -> str:
        # Never expose credentials in logs or debug output.
        return (
            f"{self.__class__.__name__}("
            f"account={self.account!r}, "
            f"user={self.user!r}, "
            f"password='***', "
            f"role={self.role!r}, "
            f"database={self.database!r}, "
            f"schema={self.schema!r}, "
            f"warehouse={self.warehouse!r})"
        )


# Convenience singleton for callers that prefer a structured object
SNOWFLAKE_CONFIG: SnowflakeConfig = SnowflakeConfig.from_env()
