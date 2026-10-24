"""Pipeline run email notification builder & SMTP dispatcher (7-step status table)."""

from __future__ import annotations

import html
import smtplib
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.config.settings import PipelineSettings, get_settings
from src.ingestion.call_weight_ledger import CallWeightLedger
from src.utils.logger import get_logger
from src.utils.step_tracker import PIPELINE_STEPS_ORDER, StepTracker

logger = get_logger(__name__)


def build_7_step_summary_rows(
    tracker: StepTracker,
    pipeline_run_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Build an ordered 7-row summary covering every major pipeline step (1..7).

    Even if a run stops at Step 3, Steps 4..7 are included with status='NOT_RUN'
    so the email table always provides a complete 7-step view.
    """
    run_id = pipeline_run_id or tracker.pipeline_run_id
    history = tracker.get_run_history(run_id)

    # Keep the latest terminal/active event per step_name for this run
    latest_by_step: Dict[str, Dict[str, Any]] = {}
    for ev in history:
        latest_by_step[ev["step_name"]] = ev

    ordered_steps = sorted(PIPELINE_STEPS_ORDER.items(), key=lambda x: x[1])
    rows: List[Dict[str, Any]] = []
    prior_failed = False

    for step_name, seq_num in ordered_steps:
        ev = latest_by_step.get(step_name)
        if ev is not None:
            status = ev.get("step_status", "UNKNOWN")
            rows_proc = int(ev.get("rows_processed", 0))
            rows_quar = int(ev.get("rows_quarantined", 0))
            duration = float(ev.get("duration_seconds", 0.0))
            err_type = ev.get("error_type")
            err_msg = ev.get("error_message")
            artifact_ref = ev.get("input_artifact_ref") or "—"

            if status == "FAILED":
                prior_failed = True
                first_line = (err_msg or "").splitlines()[0] if err_msg else "Step failed"
                details = f"{err_type}: {first_line}" if err_type else first_line
            else:
                details = Path(str(artifact_ref)).name if artifact_ref != "—" else "—"

            rows.append(
                {
                    "step_sequence_order": seq_num,
                    "step_name": step_name,
                    "step_status": status,
                    "rows_processed": rows_proc,
                    "rows_quarantined": rows_quar,
                    "duration_seconds": duration,
                    "details": details,
                    "error_type": err_type,
                    "error_message": err_msg,
                }
            )
        else:
            rows.append(
                {
                    "step_sequence_order": seq_num,
                    "step_name": step_name,
                    "step_status": "NOT_RUN",
                    "rows_processed": 0,
                    "rows_quarantined": 0,
                    "duration_seconds": 0.0,
                    "details": "Stopped at prior failed step" if prior_failed else "Not executed",
                    "error_type": None,
                    "error_message": None,
                }
            )

    return rows


def render_pipeline_run_email(
    tracker: StepTracker,
    ledger: Optional[CallWeightLedger] = None,
    settings: Optional[PipelineSettings] = None,
) -> Dict[str, Any]:
    """Render subject, plain-text body, HTML body, and save local HTML preview."""
    cfg = settings or get_settings()
    run_id = tracker.pipeline_run_id
    step_rows = build_7_step_summary_rows(tracker, run_id)

    failed_row = next((r for r in step_rows if r["step_status"] == "FAILED"), None)
    overall_status = "FAILED" if failed_row else "SUCCESS"
    total_duration = sum(r["duration_seconds"] for r in step_rows)

    # Quota summary from CallWeightLedger
    w_1m, w_1h, w_24h = (0.0, 0.0, 0.0)
    remaining_pct = 100.0
    if ledger is not None:
        w_1m, w_1h, w_24h = ledger.compute_rolling_weights()
        remaining_pct = round(
            max(0.0, (1.0 - (w_24h / cfg.api_rate_limit_per_day)) * 100.0), 2
        )

    if overall_status == "SUCCESS":
        subject = (
            f"✅ [SUCCESS] Loop IQ Weather Pipeline | Run {run_id} ({total_duration:.2f}s)"
        )
    else:
        stopped_step = failed_row["step_sequence_order"] if failed_row else "?"
        subject = (
            f"❌ [FAILED at Step {stopped_step}] Loop IQ Weather Pipeline | Run {run_id}"
        )

    status_badge_map = {
        "SUCCESS": ("#15803d", "#dcfce7", "✅ SUCCESS"),
        "SKIPPED_ON_REPAIR": ("#1d4ed8", "#dbeafe", "⏭️ SKIPPED_ON_REPAIR"),
        "FAILED": ("#b91c1c", "#fee2e2", "❌ FAILED"),
        "NOT_RUN": ("#4b5563", "#f3f4f6", "⏸️ NOT_RUN"),
        "STARTED": ("#b45309", "#fef3c7", "⏳ STARTED"),
    }

    # Build HTML table rows
    html_tr_list: List[str] = []
    text_lines: List[str] = [
        f"Loop IQ Open-Meteo Medallion Pipeline — Run Summary ({overall_status})",
        "=" * 78,
        f"Pipeline Run ID : {run_id}",
        f"Overall Status  : {overall_status}",
        f"Total Duration  : {total_duration:.2f}s",
        f"24h API Weight  : {w_24h:.2f} ({remaining_pct:.2f}% daily quota remaining)",
        "-" * 78,
        f"{'#':<3} | {'Step Name':<38} | {'Status':<18} | {'Rows':>7} | {'Quar':>5} | {'Sec':>6} | Details",
        "-" * 78,
    ]

    for r in step_rows:
        st_code = r["step_status"]
        fg_color, bg_color, badge_label = status_badge_map.get(
            st_code, ("#374151", "#f3f4f6", st_code)
        )
        html_tr_list.append(
            f"""
            <tr>
                <td style="padding:8px 10px; border:1px solid #d1d5db; text-align:center; font-weight:600;">{r['step_sequence_order']}</td>
                <td style="padding:8px 10px; border:1px solid #d1d5db; font-family:monospace;">{html.escape(r['step_name'])}</td>
                <td style="padding:8px 10px; border:1px solid #d1d5db; text-align:center;">
                    <span style="background:{bg_color}; color:{fg_color}; padding:3px 8px; border-radius:4px; font-weight:700; font-size:12px;">
                        {badge_label}
                    </span>
                </td>
                <td style="padding:8px 10px; border:1px solid #d1d5db; text-align:right;">{r['rows_processed']:,}</td>
                <td style="padding:8px 10px; border:1px solid #d1d5db; text-align:right;">{r['rows_quarantined']:,}</td>
                <td style="padding:8px 10px; border:1px solid #d1d5db; text-align:right;">{r['duration_seconds']:.2f}s</td>
                <td style="padding:8px 10px; border:1px solid #d1d5db; font-size:12px; color:{'#b91c1c' if st_code == 'FAILED' else '#374151'};">
                    {html.escape(str(r['details']))}
                </td>
            </tr>
            """
        )
        text_lines.append(
            f"{r['step_sequence_order']:<3} | {r['step_name']:<38} | {st_code:<18} | "
            f"{r['rows_processed']:>7,d} | {r['rows_quarantined']:>5,d} | "
            f"{r['duration_seconds']:>5.2f}s | {r['details']}"
        )

    text_lines.append("=" * 78)

    failure_html_block = ""
    if failed_row is not None:
        tb_escaped = html.escape(failed_row.get("error_message") or "Unknown error")
        repair_cmd = f"python -m src.cli repair --run-id {run_id}"
        failure_html_block = f"""
        <div style="margin-top:20px; padding:14px; background:#fef2f2; border-left:5px solid #dc2626; border-radius:4px;">
            <h3 style="margin:0 0 8px 0; color:#991b1b;">
                🚨 Pipeline Stopped at Step {failed_row['step_sequence_order']}: <code>{html.escape(failed_row['step_name'])}</code>
            </h3>
            <p style="margin:0 0 8px 0; font-size:13px; color:#7f1d1d;">
                <strong>Resume / Repair Command (skips already succeeded steps):</strong><br/>
                <code style="background:#fee2e2; padding:4px 8px; display:inline-block; margin-top:4px; border-radius:3px;">{html.escape(repair_cmd)}</code>
            </p>
            <pre style="background:#1f2937; color:#f9fafb; padding:10px; border-radius:4px; font-size:12px; overflow-x:auto; margin:8px 0 0 0;">{tb_escaped}</pre>
        </div>
        """
        text_lines.append(
            f"\nFAILED AT STEP {failed_row['step_sequence_order']} ({failed_row['step_name']}):"
        )
        text_lines.append(f"Repair Command: {repair_cmd}")
        text_lines.append(f"Traceback:\n{failed_row.get('error_message')}")

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    header_bg = "#15803d" if overall_status == "SUCCESS" else "#b91c1c"

    html_body = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>{html.escape(subject)}</title></head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif; color:#1f2937; max-width:960px; margin:0 auto; padding:16px;">
    <div style="background:{header_bg}; color:#ffffff; padding:14px 18px; border-radius:6px 6px 0 0;">
        <h2 style="margin:0; font-size:18px;">🏥 Loop IQ — Open-Meteo Medallion Pipeline Run Report ({overall_status})</h2>
    </div>
    <div style="border:1px solid #d1d5db; border-top:none; padding:16px; border-radius:0 0 6px 6px;">
        <table style="width:100%; margin-bottom:14px; font-size:13px;">
            <tr>
                <td><strong>Pipeline Run ID:</strong> <code>{html.escape(run_id)}</code></td>
                <td><strong>Overall Status:</strong> <strong>{overall_status}</strong></td>
                <td><strong>Total Duration:</strong> {total_duration:.2f}s</td>
            </tr>
            <tr>
                <td><strong>Reported At:</strong> {now_str}</td>
                <td colspan="2"><strong>Open-Meteo 24h Rolling Quota:</strong> {w_24h:.2f} weight used ({remaining_pct:.2f}% daily quota remaining)</td>
            </tr>
        </table>

        <h3 style="margin:12px 0 8px 0; font-size:15px;">7-Step Medallion Pipeline Execution Summary</h3>
        <table style="width:100%; border-collapse:collapse; font-size:13px;">
            <thead>
                <tr style="background:#f3f4f6; text-align:left;">
                    <th style="padding:8px 10px; border:1px solid #d1d5db; text-align:center;">#</th>
                    <th style="padding:8px 10px; border:1px solid #d1d5db;">Pipeline Step</th>
                    <th style="padding:8px 10px; border:1px solid #d1d5db; text-align:center;">Status</th>
                    <th style="padding:8px 10px; border:1px solid #d1d5db; text-align:right;">Rows Processed</th>
                    <th style="padding:8px 10px; border:1px solid #d1d5db; text-align:right;">Rows Quarantined</th>
                    <th style="padding:8px 10px; border:1px solid #d1d5db; text-align:right;">Duration</th>
                    <th style="padding:8px 10px; border:1px solid #d1d5db;">Artifact / Error Details</th>
                </tr>
            </thead>
            <tbody>
                {''.join(html_tr_list)}
            </tbody>
        </table>
        {failure_html_block}
    </div>
</body>
</html>
"""

    email_dir = cfg.log_dir / "emails"
    email_dir.mkdir(parents=True, exist_ok=True)
    preview_path = email_dir / f"email_{run_id}.html"
    preview_path.write_text(html_body, encoding="utf-8")

    return {
        "pipeline_run_id": run_id,
        "overall_status": overall_status,
        "subject": subject,
        "text_body": "\n".join(text_lines),
        "html_body": html_body,
        "preview_path": str(preview_path),
        "step_rows": step_rows,
    }


def send_pipeline_run_email(
    tracker: StepTracker,
    ledger: Optional[CallWeightLedger] = None,
    settings: Optional[PipelineSettings] = None,
) -> Dict[str, Any]:
    """Render the 7-step run report, save HTML preview to logs/emails/, and send via SMTP if enabled."""
    cfg = settings or get_settings()
    rendered = render_pipeline_run_email(tracker=tracker, ledger=ledger, settings=cfg)
    logger.info(
        "Saved 7-step pipeline run email report to %s (status=%s)",
        rendered["preview_path"],
        rendered["overall_status"],
    )

    if not cfg.alert_email_enabled:
        return {**rendered, "sent_via_smtp": False}

    if not cfg.alert_email_to or not cfg.alert_email_from:
        logger.warning(
            "ALERT_EMAIL_ENABLED=true but ALERT_EMAIL_FROM or ALERT_EMAIL_TO is empty; skipping SMTP send."
        )
        return {**rendered, "sent_via_smtp": False}

    msg = EmailMessage()
    msg["Subject"] = rendered["subject"]
    msg["From"] = cfg.alert_email_from
    recipients = [addr.strip() for addr in cfg.alert_email_to.split(",") if addr.strip()]
    msg["To"] = ", ".join(recipients)
    msg.set_content(rendered["text_body"])
    msg.add_alternative(rendered["html_body"], subtype="html")

    try:
        with smtplib.SMTP(cfg.smtp_host, cfg.smtp_port, timeout=15) as server:
            if cfg.smtp_use_tls:
                server.starttls()
            if cfg.smtp_username and cfg.smtp_password:
                server.login(cfg.smtp_username, cfg.smtp_password)
            server.send_message(msg)
        logger.info("Sent pipeline run email alert to %s", msg["To"])
        return {**rendered, "sent_via_smtp": True}
    except Exception as exc:
        logger.warning("Failed to dispatch SMTP email alert: %s", exc)
        return {**rendered, "sent_via_smtp": False, "smtp_error": str(exc)}
