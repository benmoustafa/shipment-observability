"""
observability/alerting.py

Severity-aware alerting layer.

Currently supports:
  - Console (always on — structured log output for Airflow task logs)
  - Slack incoming webhook (optional — set SLACK_WEBHOOK_URL env var to enable)

Design note on severity tiers:
  critical → halt the pipeline, send immediate alert
  warning  → alert and continue, don't block downstream tasks
  info     → log only, no alert

Slack is free with any workspace. To enable:
  1. Go to https://api.slack.com/messaging/webhooks
  2. Create an app, enable Incoming Webhooks, copy the URL
  3. Set: set SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from urllib import request, error as urllib_error

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

SLACK_WEBHOOK_URL: Optional[str] = os.getenv("SLACK_WEBHOOK_URL")

# Severity -> whether to page immediately
_ALERT_ON_SEVERITY = {"critical", "warning"}


@dataclass
class Alert:
    check_name:     str
    table_name:     str
    status:         str
    severity:       str
    observed_value: Optional[float]
    expected_value: Optional[float]
    message:        str
    run_at:         datetime


def _format_slack_message(alert: Alert) -> dict:
    """Build a Slack Block Kit message payload."""
    emoji = {"critical": ":red_circle:", "warning": ":large_yellow_circle:", "info": ":white_circle:"}
    icon  = emoji.get(alert.severity, ":white_circle:")

    header = f"{icon} *{alert.severity.upper()}* — {alert.check_name}"
    body_lines = [
        f"*Table:* `{alert.table_name}`",
        f"*Status:* `{alert.status}`",
        f"*Message:* {alert.message}",
    ]
    if alert.observed_value is not None:
        body_lines.append(f"*Observed:* {alert.observed_value:,.4f}")
    if alert.expected_value is not None:
        body_lines.append(f"*Expected (window mean):* {alert.expected_value:,.4f}")
    body_lines.append(f"*Run at:* {alert.run_at.isoformat()} UTC")

    return {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"Shipment Observability Alert"},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": header},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n".join(body_lines)},
            },
            {"type": "divider"},
        ]
    }


def _send_slack(alert: Alert) -> bool:
    """POST the alert to Slack. Returns True on success."""
    if not SLACK_WEBHOOK_URL:
        logger.debug("SLACK_WEBHOOK_URL not set — skipping Slack notification.")
        return False

    payload = json.dumps(_format_slack_message(alert)).encode("utf-8")
    req = request.Request(
        SLACK_WEBHOOK_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                logger.info("Slack alert sent for [%s]", alert.check_name)
                return True
            else:
                logger.warning("Slack returned HTTP %d for [%s]", resp.status, alert.check_name)
                return False
    except urllib_error.URLError as exc:
        logger.warning("Failed to send Slack alert for [%s]: %s", alert.check_name, exc)
        return False


def dispatch_alert(alert: Alert) -> None:
    """
    Log the alert to console and, if severity warrants it, send to Slack.
    This is the single entry point the Airflow DAG calls — it handles routing.
    """
    log_msg = (
        f"[ALERT] {alert.severity.upper()} | {alert.check_name} | "
        f"{alert.table_name} | {alert.message}"
    )
    if alert.severity == "critical":
        logger.error(log_msg)
    elif alert.severity == "warning":
        logger.warning(log_msg)
    else:
        logger.info(log_msg)

    if alert.severity in _ALERT_ON_SEVERITY:
        _send_slack(alert)


def dispatch_batch(alerts: list[Alert]) -> dict[str, int]:
    """
    Dispatch all alerts and return a summary dict.
    The Airflow BranchPythonOperator reads this to decide whether to halt.
    """
    counts = {"critical": 0, "warning": 0, "info": 0}
    for alert in alerts:
        dispatch_alert(alert)
        counts[alert.severity] = counts.get(alert.severity, 0) + 1

    logger.info(
        "Alert dispatch complete — CRITICAL: %(critical)d | WARNING: %(warning)d | INFO: %(info)d",
        counts,
    )
    return counts


def alerts_from_dbt_results(results_path: str) -> list[Alert]:
    """
    Read the dbt_test_results table and generate Alert objects for any
    non-passing tests from the most recent run.

    Used by the Airflow DAG to send Slack alerts after dbt test completes.
    """
    import pandas as pd
    from observability.db import get_engine

    engine = get_engine()
    df = pd.read_sql(
        """
        SELECT *
        FROM   dbt_test_results
        WHERE  run_at = (SELECT MAX(run_at) FROM dbt_test_results)
          AND  status NOT IN ('pass', 'skipped')
        """,
        engine,
    )

    alerts = []
    for _, row in df.iterrows():
        alerts.append(Alert(
            check_name=row["test_name"],
            table_name=row["model_name"] or "unknown",
            status=row["status"],
            severity=row["severity"],
            observed_value=float(row["failure_count"]) if row["failure_count"] else None,
            expected_value=0.0,
            message=row["message"] or "",
            run_at=row["run_at"],
        ))
    return alerts


def alerts_from_anomaly_results() -> list[Alert]:
    """
    Read anomaly_check_results for the most recent run and generate Alert objects
    for any non-passing checks.
    """
    import pandas as pd
    from observability.db import get_engine

    engine = get_engine()
    df = pd.read_sql(
        """
        SELECT *
        FROM   anomaly_check_results
        WHERE  run_at = (SELECT MAX(run_at) FROM anomaly_check_results)
          AND  status != 'pass'
        """,
        engine,
    )

    alerts = []
    for _, row in df.iterrows():
        alerts.append(Alert(
            check_name=row["check_name"],
            table_name=row["table_name"],
            status=row["status"],
            severity=row["severity"],
            observed_value=row["observed_value"],
            expected_value=row["expected_value"],
            message=row["message"] or "",
            run_at=row["run_at"],
        ))
    return alerts
