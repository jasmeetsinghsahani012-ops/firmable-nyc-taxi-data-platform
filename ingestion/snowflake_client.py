"""Reusable Snowflake client for ingestion, orchestration, and validation workloads."""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping, Sequence
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Callable, TypeVar

import snowflake.connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.errors import DatabaseError, Error, InterfaceError, OperationalError

from config.settings import SNOWFLAKE_CONFIG, SnowflakeConfig

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Snowflake error codes that commonly indicate transient infrastructure failures.
_TRANSIENT_ERROR_CODES: frozenset[int] = frozenset(
    {
        250001,  # ER_FAILED_TO_CONNECT_TO_DB
        250002,  # ER_CONNECTION_IS_CLOSED
        250003,  # ER_FAILED_TO_REQUEST
        250005,  # ER_FAILED_TO_SERVER
        251011,  # ER_CONNECTION_TIMEOUT
        601,  # SQL execution internal error (often retryable)
        604,  # SQL execution canceled (warehouse timeout / overload)
    }
)


class SnowflakeClientError(Exception):
    """Raised when a Snowflake operation fails after retries are exhausted."""

    def __init__(
        self,
        message: str,
        *,
        cause: Exception | None = None,
        sql: str | None = None,
    ) -> None:
        super().__init__(message)
        self.cause = cause
        self.sql = sql


def _is_transient_error(exc: BaseException) -> bool:
    """Return True when the exception is likely transient and safe to retry."""
    if isinstance(exc, (OperationalError, InterfaceError)):
        return True

    if isinstance(exc, DatabaseError):
        errno = getattr(exc, "errno", None)
        if errno in _TRANSIENT_ERROR_CODES:
            return True

        sqlstate = getattr(exc, "sqlstate", "")
        if sqlstate in {"08001", "08003", "08006", "57014"}:
            return True

    message = str(exc).lower()
    transient_markers = (
        "connection reset",
        "connection is closed",
        "network",
        "timeout",
        "temporarily unavailable",
        "service unavailable",
        "too many requests",
    )
    return any(marker in message for marker in transient_markers)


class SnowflakeClient:
    """Production-ready Snowflake client with retries and structured logging.

    The client reads connection settings from :mod:`config.settings` and never
    stores credentials in source code. It is safe to reuse across ingestion
    scripts, Airflow tasks, and data-quality validation modules.

    Example:
        >>> from ingestion.snowflake_client import SnowflakeClient
        >>> with SnowflakeClient() as client:
        ...     df = client.fetch_dataframe("SELECT CURRENT_VERSION()")
    """

    def __init__(
        self,
        config: SnowflakeConfig | None = None,
        *,
        autocommit: bool = True,
        max_retries: int = 3,
        retry_backoff_seconds: float = 1.0,
        retry_backoff_multiplier: float = 2.0,
    ) -> None:
        """Initialize the client.

        Args:
            config: Snowflake configuration. Defaults to ``SNOWFLAKE_CONFIG``.
            autocommit: Whether to enable autocommit on the connection.
            max_retries: Maximum number of attempts for transient failures.
            retry_backoff_seconds: Initial delay between retry attempts.
            retry_backoff_multiplier: Exponential backoff multiplier.
        """
        self._config = config or SNOWFLAKE_CONFIG
        self._autocommit = autocommit
        self._max_retries = max(1, max_retries)
        self._retry_backoff_seconds = retry_backoff_seconds
        self._retry_backoff_multiplier = retry_backoff_multiplier

        self._connection: SnowflakeConnection | None = None
        self._cursor: SnowflakeCursor | None = None

    @property
    def config(self) -> SnowflakeConfig:
        """Return the active Snowflake configuration."""
        return self._config

    @property
    def is_connected(self) -> bool:
        """Return True when an open Snowflake connection is available."""
        return self._connection is not None and not self._connection.is_closed()

    def connect(self) -> SnowflakeConnection:
        """Open a Snowflake connection using configured credentials.

        Returns:
            An active :class:`snowflake.connector.SnowflakeConnection`.

        Raises:
            SnowflakeClientError: If the connection cannot be established.
        """
        if self.is_connected:
            logger.debug("Reusing existing Snowflake connection.")
            return self._connection  # type: ignore[return-value]

        logger.info(
            "Connecting to Snowflake account=%s role=%s warehouse=%s database=%s schema=%s",
            self._config.account,
            self._config.role,
            self._config.warehouse,
            self._config.database,
            self._config.schema,
        )

        def _connect() -> SnowflakeConnection:
            return snowflake.connector.connect(
                **self._config.as_connection_params(),
                autocommit=self._autocommit,
            )

        try:
            self._connection = self._run_with_retry("connect", _connect)
        except Exception as exc:
            raise SnowflakeClientError(
                "Failed to connect to Snowflake.",
                cause=exc,
            ) from exc

        logger.info("Snowflake connection established.")
        return self._connection

    def execute(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> SnowflakeCursor:
        """Execute a SQL statement.

        Args:
            sql: SQL statement to execute.
            params: Optional positional or named bind parameters.

        Returns:
            The active Snowflake cursor after execution.

        Raises:
            SnowflakeClientError: If execution fails after retries.
        """
        self._ensure_connected()

        def _execute() -> SnowflakeCursor:
            cursor = self._get_cursor()
            logger.debug("Executing SQL statement (%d characters).", len(sql))
            cursor.execute(sql, params)
            logger.info(
                "SQL statement executed successfully. rowcount=%s sfqid=%s",
                cursor.rowcount,
                getattr(cursor, "sfqid", None),
            )
            return cursor

        try:
            return self._run_with_retry("execute", _execute, sql=sql)
        except Exception as exc:
            raise SnowflakeClientError(
                "Failed to execute SQL statement.",
                cause=exc,
                sql=sql,
            ) from exc

    def execute_many(
        self,
        sql: str,
        seq_of_parameters: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> int:
        """Execute a SQL statement against a sequence of parameter sets.

        Args:
            sql: Parameterized SQL statement.
            seq_of_parameters: Batch of bind-parameter values.

        Returns:
            The number of rows affected, as reported by the cursor.

        Raises:
            SnowflakeClientError: If batch execution fails after retries.
        """
        self._ensure_connected()

        def _execute_many() -> int:
            cursor = self._get_cursor()
            logger.debug(
                "Executing batch SQL statement (%d characters) for %d parameter sets.",
                len(sql),
                len(seq_of_parameters),
            )
            cursor.executemany(sql, seq_of_parameters)
            logger.info(
                "Batch SQL statement executed successfully. rowcount=%s sfqid=%s",
                cursor.rowcount,
                getattr(cursor, "sfqid", None),
            )
            return cursor.rowcount

        try:
            return self._run_with_retry("execute_many", _execute_many, sql=sql)
        except Exception as exc:
            raise SnowflakeClientError(
                "Failed to execute batch SQL statement.",
                cause=exc,
                sql=sql,
            ) from exc

    def fetch_dataframe(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Execute a query and return the full result set as a pandas DataFrame.

        Args:
            sql: SQL query to execute.
            params: Optional positional or named bind parameters.

        Returns:
            Query results as a pandas DataFrame.

        Raises:
            SnowflakeClientError: If the query fails after retries.
        """
        self._ensure_connected()

        def _fetch_dataframe() -> pd.DataFrame:
            cursor = self._get_cursor()
            logger.debug("Fetching DataFrame for SQL statement (%d characters).", len(sql))
            cursor.execute(sql, params)
            dataframe = cursor.fetch_pandas_all()
            logger.info(
                "Fetched DataFrame with shape=%s sfqid=%s",
                dataframe.shape,
                getattr(cursor, "sfqid", None),
            )
            return dataframe

        try:
            return self._run_with_retry("fetch_dataframe", _fetch_dataframe, sql=sql)
        except Exception as exc:
            raise SnowflakeClientError(
                "Failed to fetch query results into a DataFrame.",
                cause=exc,
                sql=sql,
            ) from exc

    def close(self) -> None:
        """Close the active cursor and Snowflake connection."""
        if self._cursor is not None:
            with suppress(Exception):
                self._cursor.close()
            self._cursor = None
            logger.debug("Snowflake cursor closed.")

        if self._connection is not None:
            with suppress(Exception):
                self._connection.close()
            self._connection = None
            logger.info("Snowflake connection closed.")

    def __enter__(self) -> SnowflakeClient:
        """Enter context manager and establish a connection."""
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager and close open resources."""
        self.close()

    def __repr__(self) -> str:
        status = "connected" if self.is_connected else "disconnected"
        return f"{self.__class__.__name__}(status={status}, config={self._config!r})"

    def _ensure_connected(self) -> SnowflakeConnection:
        """Ensure a live connection exists before running an operation."""
        if not self.is_connected:
            return self.connect()
        return self._connection  # type: ignore[return-value]

    def _get_cursor(self) -> SnowflakeCursor:
        """Return an open cursor, creating one when needed."""
        if self._cursor is None or self._cursor.is_closed():
            connection = self._ensure_connected()
            self._cursor = connection.cursor()
        return self._cursor

    def _reset_connection(self) -> None:
        """Close broken connection state before retrying an operation."""
        logger.warning("Resetting Snowflake connection after transient failure.")
        self.close()

    def _run_with_retry(
        self,
        operation: str,
        func: Callable[[], T],
        *,
        sql: str | None = None,
    ) -> T:
        """Execute ``func`` with retry logic for transient Snowflake failures."""
        delay = self._retry_backoff_seconds
        last_exception: Exception | None = None

        for attempt in range(1, self._max_retries + 1):
            try:
                return func()
            except Exception as exc:
                last_exception = exc

                if not _is_transient_error(exc) or attempt >= self._max_retries:
                    logger.error(
                        "Snowflake %s failed on attempt %d/%d.",
                        operation,
                        attempt,
                        self._max_retries,
                        exc_info=exc if isinstance(exc, Error) else True,
                    )
                    raise

                logger.warning(
                    "Transient Snowflake %s failure on attempt %d/%d: %s",
                    operation,
                    attempt,
                    self._max_retries,
                    exc,
                )
                self._reset_connection()

                if attempt < self._max_retries:
                    logger.info("Retrying Snowflake %s in %.1f seconds.", operation, delay)
                    time.sleep(delay)
                    delay *= self._retry_backoff_multiplier

        raise SnowflakeClientError(
            f"Snowflake {operation} failed after {self._max_retries} attempts.",
            cause=last_exception,
            sql=sql,
        )
