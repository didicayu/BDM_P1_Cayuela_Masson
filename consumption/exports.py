"""Export exploitation assets into analyst-facing files."""

from __future__ import annotations

import csv
import html
import json
from pathlib import Path
from typing import Any


CSV_COLUMNS = {
    "cve_prioritization": [
        "cve_id",
        "vuln_priority_score",
        "kev_listed",
        "epss_score",
        "cvss_v3_score",
        "vendor_project",
        "product",
        "date_added",
        "due_date",
    ],
    "ioc_correlations": [
        "ioc_value",
        "ioc_source",
        "ioc_type",
        "malware_family",
        "event_timestamp_utc",
        "src_ip",
        "dst_ip",
        "signature",
        "severity",
        "ingest_date",
    ],
    "anomaly_flags": [
        "event_timestamp_utc",
        "src_ip",
        "dst_ip",
        "dst_port",
        "severity",
        "signature",
        "anomaly_score",
        "anomaly_reasons",
        "ingest_date",
    ],
}


def export_consumption_outputs(
    *,
    output_dir: Path,
    vuln_priority: list[dict[str, Any]],
    ioc_correlations: list[dict[str, Any]],
    anomaly_flags: list[dict[str, Any]],
    alert_trends: list[dict[str, Any]],
    top_signatures: list[dict[str, Any]],
    top_source_ips: list[dict[str, Any]],
    top_destination_ips: list[dict[str, Any]],
    ml_model: list[dict[str, Any]],
) -> dict[str, str | int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    files = {
        "cve_prioritization": output_dir / "cve_prioritization.csv",
        "ioc_correlations": output_dir / "ioc_correlations.csv",
        "anomaly_flags": output_dir / "anomaly_flags.csv",
        "alert_trends": output_dir / "alert_trends.json",
        "soc_dashboard": output_dir / "soc_dashboard.html",
    }

    _write_csv(files["cve_prioritization"], vuln_priority, CSV_COLUMNS["cve_prioritization"])
    _write_csv(files["ioc_correlations"], ioc_correlations, CSV_COLUMNS["ioc_correlations"])
    _write_csv(files["anomaly_flags"], anomaly_flags, CSV_COLUMNS["anomaly_flags"])
    files["alert_trends"].write_text(json.dumps(alert_trends, indent=2, sort_keys=True), encoding="utf-8")
    files["soc_dashboard"].write_text(
        _dashboard_html(
            vuln_priority=vuln_priority,
            alert_trends=alert_trends,
            top_signatures=top_signatures,
            top_source_ips=top_source_ips,
            top_destination_ips=top_destination_ips,
            anomaly_flags=anomaly_flags,
            ioc_correlations=ioc_correlations,
            ml_model=ml_model,
        ),
        encoding="utf-8",
    )

    return {
        "output_dir": str(output_dir),
        "cve_rows": len(vuln_priority),
        "ioc_rows": len(ioc_correlations),
        "anomaly_rows": len(anomaly_flags),
        "alert_trend_rows": len(alert_trends),
        "top_destination_rows": len(top_destination_ips),
        "files_written": len(files),
    }


def _write_csv(path: Path, rows: list[dict[str, Any]], preferred_columns: list[str]) -> None:
    row_columns = sorted({key for row in rows for key in row})
    columns = preferred_columns + [column for column in row_columns if column not in preferred_columns]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _dashboard_html(
    *,
    vuln_priority: list[dict[str, Any]],
    alert_trends: list[dict[str, Any]],
    top_signatures: list[dict[str, Any]],
    top_source_ips: list[dict[str, Any]],
    top_destination_ips: list[dict[str, Any]],
    anomaly_flags: list[dict[str, Any]],
    ioc_correlations: list[dict[str, Any]],
    ml_model: list[dict[str, Any]],
) -> str:
    model = ml_model[0] if ml_model else {}
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>CyberSecIntel SOC Dashboard</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #18202a; }}
    h1, h2 {{ margin-bottom: 8px; }}
    .grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin: 18px 0; }}
    .metric {{ border: 1px solid #cbd5e1; padding: 12px; border-radius: 6px; }}
    table {{ border-collapse: collapse; width: 100%; margin-bottom: 22px; font-size: 13px; }}
    th, td {{ border: 1px solid #d8dee8; padding: 6px 8px; text-align: left; }}
    th {{ background: #eef2f7; }}
  </style>
</head>
<body>
  <h1>CyberSecIntel SOC Dashboard</h1>
  <div class="grid">
    <div class="metric"><strong>Prioritized CVEs</strong><br>{len(vuln_priority)}</div>
    <div class="metric"><strong>IOC Matches</strong><br>{len(ioc_correlations)}</div>
    <div class="metric"><strong>Anomaly Flags</strong><br>{len(anomaly_flags)}</div>
    <div class="metric"><strong>ML Model Rows</strong><br>{html.escape(str(model.get("trained_rows", 0)))}</div>
    <div class="metric"><strong>ML Threshold</strong><br>{html.escape(str(model.get("threshold", "")))}</div>
  </div>
  <h2>Top Vulnerabilities</h2>
  {_table(vuln_priority[:10], ["cve_id", "vuln_priority_score", "kev_listed", "epss_score", "cvss_v3_score"])}
  <h2>Alert Trends</h2>
  {_table(alert_trends[:10], ["day", "severity", "signature", "alert_count"])}
  <h2>Top Signatures</h2>
  {_table(top_signatures[:10], ["signature", "event_count"])}
  <h2>Top Source IPs</h2>
  {_table(top_source_ips[:10], ["src_ip", "event_count"])}
  <h2>Top Destination IPs</h2>
  {_table(top_destination_ips[:10], ["dst_ip", "event_count"])}
  <h2>ML Anomaly Feed</h2>
  {_table(anomaly_flags[:10], ["event_timestamp_utc", "src_ip", "dst_ip", "ml_score", "ml_threshold", "anomaly_reasons"])}
</body>
</html>
"""


def _table(rows: list[dict[str, Any]], columns: list[str]) -> str:
    if not rows:
        return "<p>No rows available yet.</p>"
    head = "".join(f"<th>{html.escape(column)}</th>" for column in columns)
    body_rows = []
    for row in rows:
        cells = "".join(f"<td>{html.escape(str(row.get(column, '')))}</td>" for column in columns)
        body_rows.append(f"<tr>{cells}</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body_rows)}</tbody></table>"
