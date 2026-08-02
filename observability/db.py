"""
observability/db.py

Shared SQLAlchemy engine factory.
Keeps connection details in one place — every module imports from here,
so credentials only ever live in a single file (or env vars in production).
"""
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ---------------------------------------------------------------------------
# Connection defaults — override via environment variables for CI / production
# ---------------------------------------------------------------------------
_DB_USER     = os.getenv("DB_USER",     "root")
_DB_PASSWORD = os.getenv("DB_PASSWORD", "Ben.2003!")
_DB_HOST     = os.getenv("DB_HOST",     "localhost")
_DB_PORT     = int(os.getenv("DB_PORT", "3306"))
_DB_NAME     = os.getenv("DB_NAME",     "shipment_observability")


def get_engine() -> Engine:
    """Return a SQLAlchemy engine connected to the shipment_observability database."""
    url = (
        f"mysql+mysqlconnector://{_DB_USER}:{_DB_PASSWORD}"
        f"@{_DB_HOST}:{_DB_PORT}/{_DB_NAME}"
    )
    return create_engine(url, pool_pre_ping=True)


def ensure_observability_tables(engine: Engine) -> None:
    """
    Create the two observability tables if they do not already exist.

    dbt_test_results     — one row per dbt test execution (schema: see below)
    anomaly_check_results — one row per statistical anomaly check execution

    Both tables share the same shape so dashboards can UNION them into a
    unified 'all checks' view without special-casing.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS dbt_test_results (
        id                  BIGINT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
        run_id              VARCHAR(64)     NOT NULL COMMENT 'Unique ID for the dbt invocation',
        run_at              DATETIME        NOT NULL COMMENT 'Wall-clock time of the dbt run',
        test_name           VARCHAR(255)    NOT NULL,
        model_name          VARCHAR(255),
        column_name         VARCHAR(255),
        status              VARCHAR(20)     NOT NULL COMMENT 'pass | warn | error | skipped',
        severity            VARCHAR(20)     NOT NULL COMMENT 'critical | warning | info',
        failure_count       INT             NOT NULL DEFAULT 0,
        execution_time_s    FLOAT,
        message             TEXT,
        compiled_sql_path   VARCHAR(512),
        INDEX idx_run_at    (run_at),
        INDEX idx_status    (status),
        INDEX idx_model     (model_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

    CREATE TABLE IF NOT EXISTS anomaly_check_results (
        id                  BIGINT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
        run_at              DATETIME        NOT NULL COMMENT 'Wall-clock time of the check',
        check_name          VARCHAR(255)    NOT NULL,
        table_name          VARCHAR(255)    NOT NULL,
        column_name         VARCHAR(255),
        status              VARCHAR(20)     NOT NULL COMMENT 'pass | warn | critical',
        severity            VARCHAR(20)     NOT NULL COMMENT 'critical | warning | info',
        observed_value      FLOAT,
        expected_value      FLOAT,
        z_score             FLOAT,
        threshold           FLOAT,
        message             TEXT,
        INDEX idx_run_at    (run_at),
        INDEX idx_status    (status),
        INDEX idx_table     (table_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with engine.connect() as conn:
        for stmt in ddl.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(text(stmt))
        conn.commit()
