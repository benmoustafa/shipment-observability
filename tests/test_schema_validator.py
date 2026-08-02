"""
Test suite for schema drift detection.

Each test constructs a specific broken DataFrame, runs the validator, and
asserts on the STRUCTURED drift event — not just "did it fail" but "did it
fail for the right reason, naming the right column, with the right severity."

This is the test suite you'd demo in an interview: "here are three ways I
deliberately broke the data, and here's the specific, actionable error each
one produces."

Test categories:
    1. Happy path — clean data passes validation.
    2. Missing column — BREAKING.
    3. Corrupted numeric — BREAKING type mismatch.
    4. Null in required field — BREAKING null violation.
    5. New unexpected column — NON-BREAKING warning.
    6. Multiple simultaneous issues — all reported, not just the first.
"""

import pandas as pd
import pytest

from ingestion.models import DriftType, Severity
from ingestion.schema_validator import SchemaValidator


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

# A minimal schema contract that covers all semantic types and both
# nullable/non-nullable columns. This is the "expected" schema that
# all test DataFrames are validated against.
MINIMAL_SCHEMA_COLUMNS = [
    {"name": "order_id", "type": "integer", "nullable": False},
    {"name": "ship_date", "type": "date", "nullable": False},
    {"name": "shipping_cost", "type": "numeric", "nullable": False},
    {"name": "delivery_status", "type": "string", "nullable": False},
    {"name": "order_profit", "type": "numeric", "nullable": True},
]


def _make_validator(
    columns=None, threshold: float = 0.95
) -> SchemaValidator:
    """Helper to build a validator from a column spec list."""
    return SchemaValidator(
        source_name="test_shipments",
        columns=columns or MINIMAL_SCHEMA_COLUMNS,
        type_match_threshold=threshold,
    )


def _make_clean_df(n_rows: int = 20) -> pd.DataFrame:
    """
    Build a clean DataFrame that passes all validation checks.

    Uses realistic-looking values so the type inference engine has
    real content to work with (not just [1, 2, 3]).
    """
    return pd.DataFrame(
        {
            "order_id": list(range(1001, 1001 + n_rows)),
            "ship_date": pd.date_range("2024-01-01", periods=n_rows).strftime(
                "%Y-%m-%d"
            ),
            "shipping_cost": [round(10.5 + i * 1.25, 2) for i in range(n_rows)],
            "delivery_status": ["Delivered"] * (n_rows // 2)
            + ["Shipped"] * (n_rows - n_rows // 2),
            "order_profit": [round(5.0 + i * 0.5, 2) for i in range(n_rows)],
        }
    )


# ---------------------------------------------------------------------------
# 1. Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    """Clean data should pass validation with zero events."""

    def test_valid_data_passes(self):
        df = _make_clean_df()
        validator = _make_validator()

        result = validator.validate(df)

        assert result.is_valid is True
        assert len(result.events) == 0
        assert len(result.breaking_events) == 0
        assert len(result.warnings) == 0

    def test_nullable_column_with_nulls_passes(self):
        """A column marked nullable=True should pass even with null values."""
        df = _make_clean_df()
        # order_profit is nullable=True in our schema
        df.loc[0:4, "order_profit"] = None

        validator = _make_validator()
        result = validator.validate(df)

        assert result.is_valid is True
        # No NULL_VIOLATION events for nullable columns
        null_events = [
            e for e in result.events if e.drift_type == DriftType.NULL_VIOLATION
        ]
        assert len(null_events) == 0


# ---------------------------------------------------------------------------
# 2. Missing column — BREAKING
# ---------------------------------------------------------------------------


class TestMissingColumn:
    """
    Scenario: an upstream system renames or drops a column.
    The validator should catch this BEFORE loading, not let it crash
    downstream in a dbt model.
    """

    def test_missing_column_is_breaking(self):
        df = _make_clean_df()
        # Simulate: upstream renamed ship_date to shipment_date
        df = df.rename(columns={"ship_date": "shipment_date"})

        validator = _make_validator()
        result = validator.validate(df)

        assert result.is_valid is False

        # Find the specific event
        missing_events = [
            e for e in result.events if e.drift_type == DriftType.MISSING_COLUMN
        ]
        assert len(missing_events) == 1

        event = missing_events[0]
        assert event.column_name == "ship_date"
        assert event.severity == Severity.BREAKING
        assert "ship_date" in event.detail
        # The detail message should be actionable — it should list available columns
        assert "shipment_date" in event.detail

    def test_multiple_missing_columns(self):
        """Dropping two columns should produce two separate BREAKING events."""
        df = _make_clean_df()
        df = df.drop(columns=["ship_date", "order_id"])

        validator = _make_validator()
        result = validator.validate(df)

        assert result.is_valid is False

        missing_events = [
            e for e in result.events if e.drift_type == DriftType.MISSING_COLUMN
        ]
        assert len(missing_events) == 2

        missing_names = {e.column_name for e in missing_events}
        assert missing_names == {"ship_date", "order_id"}


# ---------------------------------------------------------------------------
# 3. Corrupted numeric — BREAKING type mismatch
# ---------------------------------------------------------------------------


class TestCorruptedNumeric:
    """
    Scenario: a numeric column gets corrupted with text values.
    Common cause: an upstream export starts writing "N/A" or "pending"
    instead of null for missing values.
    """

    def test_heavy_corruption_is_breaking(self):
        """If >5% of values are corrupted text, the type check should fail."""
        df = _make_clean_df(n_rows=20)
        # Corrupt 30% of shipping_cost with text (6 of 20 rows)
        df["shipping_cost"] = df["shipping_cost"].astype(str)
        for i in range(6):
            df.loc[i, "shipping_cost"] = "N/A"

        validator = _make_validator()
        result = validator.validate(df)

        assert result.is_valid is False

        type_events = [
            e for e in result.events if e.drift_type == DriftType.TYPE_MISMATCH
        ]
        assert len(type_events) >= 1

        event = type_events[0]
        assert event.column_name == "shipping_cost"
        assert event.severity == Severity.BREAKING
        # Detail should mention the inferred vs expected type
        assert "numeric" in event.detail.lower() or "string" in event.detail.lower()

    def test_minor_typos_are_tolerated(self):
        """A single typo in 20 rows (5%) should NOT trigger a false failure."""
        df = _make_clean_df(n_rows=20)
        # Only 1 of 20 rows is bad (5% — at the threshold boundary)
        df["shipping_cost"] = df["shipping_cost"].astype(str)
        df.loc[0, "shipping_cost"] = "N/A"

        validator = _make_validator()
        result = validator.validate(df)

        # Should still pass — 95% of values are valid numeric
        type_events = [
            e
            for e in result.events
            if e.drift_type == DriftType.TYPE_MISMATCH
            and e.column_name == "shipping_cost"
        ]
        assert len(type_events) == 0

    def test_all_text_in_numeric_column(self):
        """Complete type change — every value is text."""
        df = _make_clean_df()
        df["shipping_cost"] = ["pending"] * len(df)

        validator = _make_validator()
        result = validator.validate(df)

        assert result.is_valid is False

        type_events = [
            e
            for e in result.events
            if e.drift_type == DriftType.TYPE_MISMATCH
            and e.column_name == "shipping_cost"
        ]
        assert len(type_events) == 1
        assert type_events[0].severity == Severity.BREAKING


# ---------------------------------------------------------------------------
# 4. Null in required field — BREAKING null violation
# ---------------------------------------------------------------------------


class TestNullViolation:
    """
    Scenario: a column that should NEVER be null starts having nulls.
    This is different from a type mismatch — the type is correct, but
    the business constraint is violated.
    """

    def test_null_in_required_field_is_breaking(self):
        df = _make_clean_df(n_rows=20)
        # ship_date is non-nullable — introduce 30% nulls
        df.loc[0:5, "ship_date"] = None

        validator = _make_validator()
        result = validator.validate(df)

        assert result.is_valid is False

        null_events = [
            e for e in result.events if e.drift_type == DriftType.NULL_VIOLATION
        ]
        assert len(null_events) == 1

        event = null_events[0]
        assert event.column_name == "ship_date"
        assert event.severity == Severity.BREAKING
        # Detail should quantify the damage
        assert "6" in event.detail  # 6 null rows (index 0–5)
        assert "20" in event.detail  # out of 20 total

    def test_single_null_in_required_field(self):
        """Even one null in a non-nullable field is BREAKING."""
        df = _make_clean_df(n_rows=20)
        df.loc[0, "ship_date"] = None

        validator = _make_validator()
        result = validator.validate(df)

        null_events = [
            e
            for e in result.events
            if e.drift_type == DriftType.NULL_VIOLATION
            and e.column_name == "ship_date"
        ]
        assert len(null_events) == 1
        assert null_events[0].severity == Severity.BREAKING


# ---------------------------------------------------------------------------
# 5. New unexpected column — NON-BREAKING
# ---------------------------------------------------------------------------


class TestNewColumn:
    """
    Scenario: upstream adds a new column we don't know about.
    This shouldn't break the load — existing queries don't reference it —
    but we should log it as a warning for schema review.
    """

    def test_new_column_is_non_breaking(self):
        df = _make_clean_df()
        df["internal_notes"] = "test note"

        validator = _make_validator()
        result = validator.validate(df)

        # Should still be valid — new columns don't break anything
        assert result.is_valid is True

        # But there should be a warning event
        new_events = [
            e for e in result.events if e.drift_type == DriftType.NEW_COLUMN
        ]
        assert len(new_events) == 1

        event = new_events[0]
        assert event.column_name == "internal_notes"
        assert event.severity == Severity.NON_BREAKING

    def test_multiple_new_columns(self):
        df = _make_clean_df()
        df["internal_notes"] = "note"
        df["debug_flag"] = True

        validator = _make_validator()
        result = validator.validate(df)

        assert result.is_valid is True

        new_events = [
            e for e in result.events if e.drift_type == DriftType.NEW_COLUMN
        ]
        assert len(new_events) == 2


# ---------------------------------------------------------------------------
# 6. Multiple simultaneous issues — all reported
# ---------------------------------------------------------------------------


class TestMultipleIssues:
    """
    The validator MUST NOT stop at the first problem. In production, you
    want to know ALL the issues in one pass so you can fix them all
    before retrying, instead of a frustrating fix-one-discover-another loop.
    """

    def test_multiple_issues_all_reported(self):
        """
        Combine two BREAKING issues:
          - ship_date column is renamed (→ MISSING_COLUMN)
          - shipping_cost is all text (→ TYPE_MISMATCH)
        Both should be reported.
        """
        df = _make_clean_df()
        df = df.rename(columns={"ship_date": "shipment_date"})
        df["shipping_cost"] = ["pending"] * len(df)

        validator = _make_validator()
        result = validator.validate(df)

        assert result.is_valid is False
        assert len(result.breaking_events) >= 2

        drift_types = {e.drift_type for e in result.breaking_events}
        assert DriftType.MISSING_COLUMN in drift_types
        assert DriftType.TYPE_MISMATCH in drift_types

    def test_breaking_and_non_breaking_together(self):
        """
        One BREAKING issue (null violation) + one NON-BREAKING (new column).
        The load should be halted, but BOTH events should be reported.
        """
        df = _make_clean_df()
        df.loc[0:5, "order_id"] = None  # BREAKING
        df["extra_col"] = "test"  # NON-BREAKING

        validator = _make_validator()
        result = validator.validate(df)

        assert result.is_valid is False
        assert len(result.breaking_events) >= 1
        assert len(result.warnings) >= 1

        # Total events should include both
        assert len(result.events) >= 2
