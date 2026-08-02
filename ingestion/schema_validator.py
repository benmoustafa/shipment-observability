"""
Schema validator — compares an incoming DataFrame against a declared YAML schema
and produces structured drift events.

Design principle: **collect everything, decide once**. All four checks
(missing columns, new columns, type mismatches, null violations) run to
completion before `is_valid` is evaluated. This gives the operator (or the
Phase 5 dashboard) the full picture in one pass, rather than a frustrating
fix-one-rerun-discover-another cycle.

Usage:
    from ingestion.schema_validator import SchemaValidator

    validator = SchemaValidator.from_yaml("ingestion/schemas/shipments.yaml")
    result = validator.validate(df)
    if not result.is_valid:
        raise SchemaValidationError(result)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import yaml

from ingestion.models import DriftEvent, DriftType, Severity, ValidationResult
from ingestion.type_inference import infer_semantic_type, is_type_compatible


class SchemaValidator:
    """
    Validates a DataFrame against a YAML schema contract.

    The validator is stateless after construction — you can call `.validate()`
    on multiple DataFrames (e.g., daily files) with the same instance.
    """

    def __init__(
        self,
        source_name: str,
        columns: List[Dict[str, Any]],
        type_match_threshold: float = 0.95,
    ):
        """
        Args:
            source_name: Identifier for the source table (used in drift events).
            columns: List of column specs from the YAML config. Each is a dict
                     with keys: name, type, nullable, and optionally
                     type_match_threshold (per-column override).
            type_match_threshold: Global default for type inference confidence.
        """
        self.source_name = source_name
        self.columns = columns
        self.type_match_threshold = type_match_threshold

        # Build a lookup for quick access during validation.
        # Using a dict keyed by column name — O(1) lookups instead of O(n) scans.
        self._expected_columns: Dict[str, Dict[str, Any]] = {
            col["name"]: col for col in columns
        }

    @classmethod
    def from_yaml(cls, schema_path: str | Path) -> SchemaValidator:
        """
        Factory: construct a validator from a YAML schema file.

        This is the primary constructor — keeps YAML parsing out of the
        validation logic so the validator is testable with plain dicts too.
        """
        schema_path = Path(schema_path)
        with open(schema_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        return cls(
            source_name=config["source_name"],
            columns=config["columns"],
            type_match_threshold=config.get("type_match_threshold", 0.95),
        )

    def validate(self, df: pd.DataFrame) -> ValidationResult:
        """
        Run all validation checks and return a ValidationResult.

        Check order:
            1. Missing columns (BREAKING)
            2. New/unexpected columns (NON-BREAKING)
            3. Type mismatches (BREAKING) — only for columns that exist
            4. Null violations (BREAKING) — only for columns that exist
                and are marked nullable=false

        All checks run to completion — we never short-circuit.
        """
        events: List[DriftEvent] = []

        actual_columns = set(df.columns)
        expected_columns = set(self._expected_columns.keys())

        # --- 1. Missing columns ---
        events.extend(self._check_missing_columns(actual_columns, expected_columns))

        # --- 2. New/unexpected columns ---
        events.extend(self._check_new_columns(actual_columns, expected_columns))

        # --- 3 & 4. Type + null checks (only for columns that actually exist) ---
        # We skip these for missing columns — no point checking the type
        # of a column that isn't there. The MISSING_COLUMN event already
        # flagged it as BREAKING.
        present_expected = expected_columns & actual_columns
        for col_name in sorted(present_expected):
            col_spec = self._expected_columns[col_name]

            events.extend(self._check_column_type(df[col_name], col_spec))
            events.extend(self._check_null_constraint(df[col_name], col_spec))

        return ValidationResult(source_table=self.source_name, events=events)

    def _check_missing_columns(
        self, actual: set, expected: set
    ) -> List[DriftEvent]:
        """
        Flag expected columns that are absent from the incoming data.

        This is always BREAKING — a missing column means downstream SQL,
        dbt models, and dashboards will fail or produce wrong results.
        """
        events = []
        missing = sorted(expected - actual)

        for col_name in missing:
            events.append(
                DriftEvent(
                    source_table=self.source_name,
                    column_name=col_name,
                    drift_type=DriftType.MISSING_COLUMN,
                    severity=Severity.BREAKING,
                    detail=(
                        f"Expected column '{col_name}' is missing from the "
                        f"incoming data. Available columns: "
                        f"{sorted(actual)}"
                    ),
                )
            )

        return events

    def _check_new_columns(
        self, actual: set, expected: set
    ) -> List[DriftEvent]:
        """
        Flag columns present in the data but not in the schema contract.

        This is NON-BREAKING — new columns won't break existing queries, but
        they signal that the upstream source changed and the schema contract
        should be reviewed.
        """
        events = []
        new_cols = sorted(actual - expected)

        for col_name in new_cols:
            events.append(
                DriftEvent(
                    source_table=self.source_name,
                    column_name=col_name,
                    drift_type=DriftType.NEW_COLUMN,
                    severity=Severity.NON_BREAKING,
                    detail=(
                        f"Unexpected column '{col_name}' found in incoming "
                        f"data but not declared in the schema contract. "
                        f"Consider adding it to the schema if it should be "
                        f"tracked."
                    ),
                )
            )

        return events

    def _check_column_type(
        self, series: pd.Series, col_spec: Dict[str, Any]
    ) -> List[DriftEvent]:
        """
        Infer the semantic type of a column and compare to the expected type.

        Uses the tolerance threshold: if the inferred type matches but confidence
        is below the threshold, that means too many values failed to parse —
        effectively a type corruption.

        Compatibility is checked via is_type_compatible() — e.g., a column
        declared as "numeric" that infers as "integer" is fine (int ⊂ float).
        """
        # If the column is completely empty/null, we cannot infer its type.
        # We skip type validation; the nullability constraint (handled in
        # _check_null_constraint) will enforce if this column is allowed to be null.
        if series.dropna().empty:
            return []

        events = []
        expected_type = col_spec["type"]
        col_name = col_spec["name"]

        # Per-column threshold override, falling back to global default.
        threshold = col_spec.get("type_match_threshold", self.type_match_threshold)

        inferred_type, confidence = infer_semantic_type(series, threshold)

        # Check compatibility: is the actual type a valid match for the expected?
        if not is_type_compatible(inferred_type, expected_type):
            events.append(
                DriftEvent(
                    source_table=self.source_name,
                    column_name=col_name,
                    drift_type=DriftType.TYPE_MISMATCH,
                    severity=Severity.BREAKING,
                    detail=(
                        f"Expected type '{expected_type}' but inferred "
                        f"'{inferred_type}' (confidence: {confidence:.1%}). "
                        f"This suggests the column's content has changed — "
                        f"e.g., a numeric field now contains text values."
                    ),
                )
            )
        elif confidence < threshold:
            # Type matches but too many values failed to parse.
            # This catches partial corruption: the column is "mostly numeric"
            # but has enough bad values to be suspicious.
            events.append(
                DriftEvent(
                    source_table=self.source_name,
                    column_name=col_name,
                    drift_type=DriftType.TYPE_MISMATCH,
                    severity=Severity.BREAKING,
                    detail=(
                        f"Column type is '{expected_type}' as expected, but "
                        f"only {confidence:.1%} of non-null values parsed "
                        f"successfully (threshold: {threshold:.1%}). "
                        f"This suggests data corruption — too many unparseable "
                        f"values."
                    ),
                )
            )

        return events

    def _check_null_constraint(
        self, series: pd.Series, col_spec: Dict[str, Any]
    ) -> List[DriftEvent]:
        """
        Check that non-nullable columns don't contain null values.

        This is separate from type checking — a column can have the right type
        but still violate a business constraint that it should never be empty.
        E.g., every shipment must have a ship_date.
        """
        events = []
        col_name = col_spec["name"]
        nullable = col_spec.get("nullable", True)  # Default to allowing nulls.

        if not nullable:
            null_count = int(series.isna().sum())
            total_count = len(series)

            if null_count > 0:
                null_pct = null_count / total_count
                events.append(
                    DriftEvent(
                        source_table=self.source_name,
                        column_name=col_name,
                        drift_type=DriftType.NULL_VIOLATION,
                        severity=Severity.BREAKING,
                        detail=(
                            f"Column '{col_name}' is declared as non-nullable, "
                            f"but {null_count:,} of {total_count:,} rows "
                            f"({null_pct:.1%}) contain null values. "
                            f"This violates the data contract."
                        ),
                    ),
                )

        return events
