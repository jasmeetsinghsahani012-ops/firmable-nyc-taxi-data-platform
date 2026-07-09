from __future__ import annotations

import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt"


def run_dbt_command(command: list[str]) -> None:
    """Run a dbt command and fail explicitly if dbt returns non-zero status."""
    logger.info("Running dbt command: %s", " ".join(command))

    result = subprocess.run(
        command,
        cwd=str(DBT_PROJECT_DIR),
        text=True,
        capture_output=True,
        check=False,
    )

    logger.info("dbt stdout:\n%s", result.stdout)

    if result.stderr:
        logger.warning("dbt stderr:\n%s", result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"dbt command failed: {' '.join(command)}")


def dbt_run() -> None:
    run_dbt_command(["dbt", "run"])


def dbt_test() -> None:
    run_dbt_command(["dbt", "test"])