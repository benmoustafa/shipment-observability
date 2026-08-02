"""
Semantic type inference engine.

The core insight: pandas' dtype tells you how data is *stored* (object, int64,
float64), not what it *means*. A column of integers with one "N/A" typo has
dtype=object, but its semantic type is still "integer" — just with a dirty value.

This module infers semantic types by actually attempting to parse values and
measuring what fraction succeed. The caller passes a threshold (e.g., 0.95)
and we report both the inferred type and the confidence (parse success rate).
"""

from __future__ import annotations

from typing import Tuple

import pandas as pd


# Type hierarchy used for compatibility checks.
# "integer" is a subtype of "numeric" — if you expected numeric and got integer,
# that's fine (an int column is always a valid float column).
_COMPATIBLE_TYPES = {
    "integer": {"integer", "numeric"},
    "numeric": {"numeric"},
    "date": {"date"},
    "boolean": {"boolean", "integer", "numeric"},
    "string": {"string"},
}


def is_type_compatible(actual: str, expected: str) -> bool:
    """
    Check if `actual` inferred type is compatible with `expected` declared type.

    Examples:
        - actual="integer", expected="numeric" → True (int is a valid float)
        - actual="string",  expected="numeric" → False (corruption)
        - actual="numeric", expected="integer" → False (precision changed)
    """
    compatible_with = _COMPATIBLE_TYPES.get(actual, set())
    return expected in compatible_with


def infer_semantic_type(
    series: pd.Series, threshold: float = 0.95
) -> Tuple[str, float]:
    """
    Infer the semantic type of a pandas Series from its actual values.

    Tries parsers in a specific order (most restrictive first) and returns
    the first type where the parse success rate exceeds `threshold`.

    Args:
        series: The column data to inspect.
        threshold: Fraction of non-null values that must parse successfully
                   for a type to be accepted (default 0.95).

    Returns:
        Tuple of (inferred_type, confidence):
        - inferred_type: one of "boolean", "integer", "numeric", "date", "string"
        - confidence: fraction of non-null values that parsed successfully
                      for the winning type (1.0 for "string" since everything
                      is a string).

    Why this order?
        1. Boolean first — smallest value set, unambiguous.
        2. Integer before numeric — integers are a subset of floats,
           so if it's integer we want to catch that specifically.
        3. Numeric before date — some numeric IDs could look like dates
           (e.g., 20210115 could be a date OR an integer).
        4. Date before string — dates are structured text.
        5. String is the universal fallback — everything is a string.
    """
    # Drop nulls — we only care about the values that are present.
    # Null checks are handled separately by the validator.
    non_null = series.dropna()

    # Edge case: if the column is entirely null, we can't infer anything.
    # Report it as string with 0 confidence — the null-check will catch it
    # if the column is required.
    if len(non_null) == 0:
        return ("string", 0.0)

    total = len(non_null)

    # --- 1. Boolean ---
    bool_rate = _try_boolean(non_null, total)
    if bool_rate >= threshold:
        return ("boolean", bool_rate)

    # --- 2. Integer ---
    int_rate = _try_integer(non_null, total)
    if int_rate >= threshold:
        return ("integer", int_rate)

    # --- 3. Numeric (float) ---
    num_rate = _try_numeric(non_null, total)
    if num_rate >= threshold:
        return ("numeric", num_rate)

    # --- 4. Date ---
    date_rate = _try_date(non_null, total)
    if date_rate >= threshold:
        return ("date", date_rate)

    # --- 5. Fallback: string ---
    return ("string", 1.0)


def _try_boolean(non_null: pd.Series, total: int) -> float:
    """
    Check if values map to a small boolean-like set.

    We normalize to lowercase strings before checking. This catches
    columns with values like "True"/"False", "1"/"0", "yes"/"no".
    """
    bool_values = {"true", "false", "1", "0", "yes", "no", "t", "f", "y", "n"}
    as_str = non_null.astype(str).str.strip().str.lower()
    matches = as_str.isin(bool_values).sum()
    return matches / total


def _try_integer(non_null: pd.Series, total: int) -> float:
    """
    Check if values are whole numbers.

    Strategy: convert to numeric first, then check if all successful
    conversions have no fractional part. This handles string-encoded
    integers like "42" as well as actual int/float dtypes.
    """
    numeric = pd.to_numeric(non_null, errors="coerce")
    successfully_parsed = numeric.dropna()

    if len(successfully_parsed) == 0:
        return 0.0

    # Check if all parsed values are whole numbers (no fractional part).
    # We use modulo rather than dtype check because pandas sometimes
    # stores integers as float64 when there are nulls.
    are_whole = (successfully_parsed % 1 == 0).all()

    if are_whole:
        return len(successfully_parsed) / total
    return 0.0


def _try_numeric(non_null: pd.Series, total: int) -> float:
    """
    Check if values parse as numbers (integers or floats).

    This is simpler than integer — we just need pd.to_numeric to succeed.
    """
    numeric = pd.to_numeric(non_null, errors="coerce")
    parsed_count = numeric.notna().sum()
    return parsed_count / total


def _try_date(non_null: pd.Series, total: int) -> float:
    """
    Check if values parse as dates.

    We use pandas' flexible date parser with `errors='coerce'` and
    `infer_datetime_format=True` for speed. The DataCo dataset has
    mixed date formats, so flexibility here is intentional.

    We also guard against numeric-looking values being falsely detected
    as dates (e.g., 20210115 → 2021-01-15). If > threshold of values
    successfully parse as numeric, we skip the date check entirely.
    """
    # Guard: if it looks numeric, don't try date parsing.
    num_rate = _try_numeric(non_null, total)
    if num_rate >= 0.5:
        return 0.0

    dates = pd.to_datetime(non_null, errors="coerce", format="mixed")
    parsed_count = dates.notna().sum()
    return parsed_count / total
