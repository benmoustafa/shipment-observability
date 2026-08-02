"""
Data loader — the thin orchestration layer that ties CSV reading, schema
validation, and loading together.

This module is intentionally thin. Its job:
    1. Read CSV into a DataFrame.
    2. Hand it to the SchemaValidator.
    3. Log all drift events (structured, to console for now).
    4. If valid → return the DataFrame (later: .to_sql()).
    5. If invalid → raise SchemaValidationError with the full report.

The actual intelligence lives in schema_validator.py and type_inference.py.
This separation matters for Phase 4: Airflow will call the validator as its
own task (BranchPythonOperator), decoupled from file I/O.

Usage:
    # As a library:
    from ingestion.loader import load_csv

    df, result = load_csv(
        csv_path="data/raw/shipments.csv",
        schema_path="ingestion/schemas/shipments.yaml"
    )

    # As a script:
    python -m ingestion.loader data/raw/shipments.csv ingestion/schemas/shipments.yaml
"""

import os
import logging
import sys
from pathlib import Path
from typing import Tuple

import pandas as pd
import sqlalchemy as sa

from ingestion.models import Severity, ValidationResult
from ingestion.schema_validator import SchemaValidator

# Configure a structured logger rather than using print().
# In production you'd use structlog or python-json-logger, but stdlib
# logging is fine for a portfolio project — the key is NOT using print().
logger = logging.getLogger("ingestion.loader")


class SchemaValidationError(Exception):
    """
    Raised when BREAKING schema drift is detected.

    Carries the full ValidationResult so callers (Airflow, tests) can
    inspect structured events — not just parse an error string.
    """

    def __init__(self, result: ValidationResult):
        self.result = result
        super().__init__(result.summary())


def get_db_engine() -> sa.Engine:
    """Create a SQLAlchemy connection engine using environment variables or local MySQL defaults."""
    db_user = os.environ.get("DB_USER", "root")
    db_pass = os.environ.get("DB_PASSWORD", "Ben.2003!")
    db_host = os.environ.get("DB_HOST", "localhost")
    db_port = os.environ.get("DB_PORT", "3306")
    db_name = os.environ.get("DB_NAME", "shipment_observability")
    
    connection_uri = f"mysql+mysqlconnector://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    return sa.create_engine(connection_uri)


def run_ingestion(csv_path: str | Path, schema_path: str | Path = None) -> ValidationResult:
    """
    Ingestion entry point called by the Airflow DAG and Windows runner.
    """
    if schema_path is None:
        schema_path = Path(__file__).resolve().parent / "schemas" / "shipments.yaml"
    try:
        _, result = load_csv(csv_path, schema_path)
        return result
    except SchemaValidationError as e:
        return e.result


def load_csv(
    csv_path: str | Path,
    schema_path: str | Path,
    encoding: str = "utf-8",
) -> Tuple[pd.DataFrame, ValidationResult]:
    """
    Load a CSV file with schema validation.

    Args:
        csv_path: Path to the incoming CSV file.
        schema_path: Path to the YAML schema contract.
        encoding: Encoding to use when reading the CSV file.

    Returns:
        Tuple of (DataFrame, ValidationResult) on success.

    Raises:
        FileNotFoundError: If the CSV or schema file doesn't exist.
        SchemaValidationError: If BREAKING drift is detected.
            The exception carries the full ValidationResult with all
            events (breaking and non-breaking).
    """
    csv_path = Path(csv_path)
    schema_path = Path(schema_path)

    # --- 1. Read the CSV ---
    logger.info("Reading CSV: %s", csv_path)
    try:
        df = pd.read_csv(csv_path, encoding=encoding)
    except UnicodeDecodeError:
        logger.warning("Failed to decode with %s. Retrying with 'latin-1' encoding.", encoding)
        df = pd.read_csv(csv_path, encoding="latin-1")
    logger.info("Loaded %d rows, %d columns", len(df), len(df.columns))

    # --- 2. Build the validator from the schema contract ---
    validator = SchemaValidator.from_yaml(schema_path)

    # --- 3. Validate ---
    logger.info("Validating against schema: %s", schema_path.name)
    result = validator.validate(df)

    # --- 4. Log every event (structured) ---
    _log_drift_events(result)

    # --- 5. Persist drift events (breaking or warnings) to database ---
    if result.events:
        logger.info("Logging %d drift event(s) to 'schema_drift_log' table...", len(result.events))
        try:
            engine = get_db_engine()
            events_df = pd.DataFrame([event.to_dict() for event in result.events])
            events_df.to_sql("schema_drift_log", con=engine, if_exists="append", index=False)
            logger.info("Drift events persisted successfully.")
        except Exception as e:
            logger.error("Failed to persist drift events to database: %s", str(e))

    # --- 6. Decide: proceed or halt ---
    if not result.is_valid:
        logger.error(
            "Schema validation FAILED — %d BREAKING issue(s) detected. "
            "Load halted.",
            len(result.breaking_events),
        )
        raise SchemaValidationError(result)

    if result.warnings:
        logger.warning(
            "Schema validation passed with %d non-breaking warning(s). "
            "Proceeding with load.",
            len(result.warnings),
        )
    else:
        logger.info("Schema validation passed — no drift detected.")

    # --- 7. Load shipments data to database raw_shipments table ---
    logger.info("Loading validated shipments data to 'raw_shipments' table (chunksize=10000)...")
    try:
        engine = get_db_engine()
        df.to_sql("raw_shipments", con=engine, if_exists="replace", index=False, chunksize=10000)
        logger.info("Data loaded successfully! Table 'raw_shipments' updated.")
    except Exception as e:
        logger.error("Failed to load shipments data to database: %s", str(e))
        raise

    return df, result


def _log_drift_events(result: ValidationResult) -> None:
    """
    Log each drift event at the appropriate level.

    BREAKING events → ERROR level
    NON_BREAKING events → WARNING level

    Each event is logged as a structured string (not a raw dict) so it's
    human-readable in the console but still parseable. In production,
    you'd emit these as JSON via structlog.
    """
    for event in result.events:
        if event.severity == Severity.BREAKING:
            logger.error(str(event))
        else:
            logger.warning(str(event))


def _configure_console_logging() -> None:
    """Set up human-readable console logging for CLI usage."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    root_logger = logging.getLogger("ingestion")
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(handler)


# --- CLI entry point ---
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(
            "Usage: python -m ingestion.loader <csv_path> <schema_path>",
            file=sys.stderr,
        )
        sys.exit(1)

    _configure_console_logging()

    try:
        df, result = load_csv(sys.argv[1], sys.argv[2])
        print(result.summary())
        print(f"\n[PASS] Successfully loaded {len(df):,} rows.")
    except SchemaValidationError as e:
        print(e.result.summary(), file=sys.stderr)
        sys.exit(1)
