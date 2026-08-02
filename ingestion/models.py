"""
Data models for schema drift detection.

These dataclasses define the structured "vocabulary" that the validator
produces and the loader/orchestrator consumes. Using enums + dataclasses
(rather than raw dicts) catches typos at definition time and makes test
assertions readable.

Later phases will persist DriftEvent records to a Postgres
`schema_drift_log` table via `pd.DataFrame([e.to_dict() for e in events]).to_sql()`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import List


class DriftType(str, Enum):
    """
    What kind of schema deviation was detected.

    Inheriting from `str` lets us do `DriftType.MISSING_COLUMN == "MISSING_COLUMN"`
    and also makes JSON/CSV serialization trivial.
    """

    MISSING_COLUMN = "MISSING_COLUMN"
    NEW_COLUMN = "NEW_COLUMN"
    TYPE_MISMATCH = "TYPE_MISMATCH"
    NULL_VIOLATION = "NULL_VIOLATION"


class Severity(str, Enum):
    """
    BREAKING  — the loader must refuse to load.
    NON_BREAKING — log a warning, proceed with the load.
    """

    BREAKING = "BREAKING"
    NON_BREAKING = "NON_BREAKING"


@dataclass
class DriftEvent:
    """
    One structured record per detected schema deviation.

    Designed to be directly insertable into a Postgres table later — every
    field maps to a column, and `.to_dict()` handles serialization.
    """

    source_table: str
    column_name: str
    drift_type: DriftType
    severity: Severity
    detail: str
    detected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict:
        """Serialize for Postgres insertion or JSON logging."""
        return {
            "source_table": self.source_table,
            "column_name": self.column_name,
            "drift_type": self.drift_type.value,
            "severity": self.severity.value,
            "detail": self.detail,
            "detected_at": self.detected_at.isoformat(),
        }

    def __str__(self) -> str:
        """Human-readable one-liner for console/log output."""
        return (
            f"[{self.severity.value}] {self.drift_type.value} on "
            f"'{self.source_table}.{self.column_name}': {self.detail}"
        )


@dataclass
class ValidationResult:
    """
    Aggregates all DriftEvents from a single validation pass.

    The loader checks `is_valid` to decide whether to proceed or halt.
    Airflow (Phase 4) will use `breaking_events` and `warnings` to drive
    branching logic.
    """

    source_table: str
    events: List[DriftEvent] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        """True only if there are zero BREAKING events."""
        return not any(e.severity == Severity.BREAKING for e in self.events)

    @property
    def breaking_events(self) -> List[DriftEvent]:
        """All events that would halt the load."""
        return [e for e in self.events if e.severity == Severity.BREAKING]

    @property
    def warnings(self) -> List[DriftEvent]:
        """Non-breaking events — load proceeds, but these get logged."""
        return [e for e in self.events if e.severity == Severity.NON_BREAKING]

    def summary(self) -> str:
        """
        Multi-line human-readable summary of all drift events.

        This is what the loader prints to console on failure — it should be
        actionable enough that someone reading the CI log knows exactly
        what broke without digging into code.
        """
        if not self.events:
            return f"[PASS] Schema validation passed for '{self.source_table}' — no drift detected."

        lines = [
            f"Schema validation for '{self.source_table}': "
            f"{len(self.breaking_events)} BREAKING, {len(self.warnings)} NON-BREAKING"
        ]
        lines.append("=" * 70)

        for event in self.events:
            lines.append(str(event))

        lines.append("=" * 70)

        if self.is_valid:
            lines.append(
                "[WARNING] Non-breaking drift detected — loading will proceed, "
                "but review the warnings above."
            )
        else:
            lines.append(
                "[HALT] BREAKING drift detected — load HALTED. "
                "Fix the issues above before retrying."
            )

        return "\n".join(lines)
