"""
export.py — CSV export utility for Transaction Monitoring Platform.

Exports transactions, alerts, and customer summaries to CSV files in output/.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

import db

OUTPUT_DIR = Path(__file__).parent / "output"


def export_transactions(conn, limit: int | None = None) -> Path:
    """Export all transactions with enrichment data to CSV.

    Args:
        conn:  Open database connection.
        limit: Optional row limit (for testing).

    Returns:
        Path to the exported CSV file.
    """
    path = OUTPUT_DIR / "transactions.csv"
    limit_clause = f"LIMIT {limit}" if limit else ""
    rows = conn.execute(f"""
        SELECT t.id, t.account_id, t.transaction_type, t.amount, t.currency,
               t.timestamp, t.counterparty_account, t.counterparty_name,
               t.counterparty_country, t.channel, t.status,
               te.amount_usd, te.is_round_amount, te.is_large_cash,
               te.velocity_1h, te.velocity_24h, te.velocity_7d,
               te.amount_velocity_24h, te.country_risk_score,
               te.is_new_counterparty, te.account_age_days,
               c.id AS customer_id, c.name AS customer_name,
               c.risk_rating, c.customer_type
        FROM transactions t
        LEFT JOIN transaction_enrichment te ON te.transaction_id = t.id
        JOIN accounts a ON a.id = t.account_id
        JOIN customers c ON c.id = a.customer_id
        ORDER BY t.timestamp
        {limit_clause}
    """).fetchall()

    with open(path, "w", newline="", encoding="utf-8") as f:
        if rows:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows([dict(r) for r in rows])

    print(f"[export] Transactions: {len(rows):,} rows -> {path}")
    return path


def export_alerts(conn, limit: int | None = None) -> Path:
    """Export all alerts with rule and customer details to CSV.

    Args:
        conn:  Open database connection.
        limit: Optional row limit.

    Returns:
        Path to the exported CSV file.
    """
    path = OUTPUT_DIR / "alerts.csv"
    limit_clause = f"LIMIT {limit}" if limit else ""
    rows = conn.execute(f"""
        SELECT al.id, al.score, al.status, al.created_date, al.resolved_date,
               r.name AS rule_name, r.category, r.severity,
               t.id AS transaction_id, t.transaction_type, t.amount, t.currency,
               t.timestamp, t.counterparty_country,
               c.id AS customer_id, c.name AS customer_name,
               c.risk_rating, c.customer_type, c.pep_status,
               asc2.rule_score, asc2.customer_risk_score,
               asc2.geographic_risk_score, asc2.behavioral_score
        FROM alerts al
        JOIN rules r ON r.id = al.rule_id
        JOIN transactions t ON t.id = al.transaction_id
        JOIN accounts a ON a.id = t.account_id
        JOIN customers c ON c.id = a.customer_id
        LEFT JOIN alert_scores asc2 ON asc2.alert_id = al.id
        ORDER BY al.score DESC
        {limit_clause}
    """).fetchall()

    with open(path, "w", newline="", encoding="utf-8") as f:
        if rows:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows([dict(r) for r in rows])

    print(f"[export] Alerts: {len(rows):,} rows -> {path}")
    return path


def export_customer_risk_summary(conn) -> Path:
    """Export per-customer risk summary to CSV.

    Args:
        conn: Open database connection.

    Returns:
        Path to the exported CSV file.
    """
    path = OUTPUT_DIR / "customer_risk_summary.csv"
    rows = conn.execute("""
        SELECT
            c.id, c.name, c.customer_type, c.risk_rating,
            c.country, c.kyc_status, c.pep_status, c.registration_date,
            cr.risk_level AS country_risk_level,
            COUNT(DISTINCT a.id)  AS account_count,
            COUNT(DISTINCT t.id)  AS transaction_count,
            ROUND(SUM(te.amount_usd), 2) AS total_volume_usd,
            COUNT(DISTINCT al.id) AS total_alerts,
            ROUND(MAX(al.score), 2) AS max_alert_score,
            ROUND(AVG(al.score), 2) AS avg_alert_score
        FROM customers c
        JOIN country_risk cr ON cr.country_code = c.country
        LEFT JOIN accounts a ON a.customer_id = c.id
        LEFT JOIN transactions t ON t.account_id = a.id
        LEFT JOIN transaction_enrichment te ON te.transaction_id = t.id
        LEFT JOIN alerts al ON al.transaction_id = t.id
        GROUP BY c.id
        ORDER BY max_alert_score DESC NULLS LAST, total_alerts DESC
    """).fetchall()

    with open(path, "w", newline="", encoding="utf-8") as f:
        if rows:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows([dict(r) for r in rows])

    print(f"[export] Customer risk summary: {len(rows):,} rows -> {path}")
    return path


def main() -> None:
    """Run all exports."""
    print("=" * 60)
    print("Transaction Monitor — CSV Export")
    print("=" * 60)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    conn = db.get_connection()
    try:
        export_transactions(conn)
        export_alerts(conn)
        export_customer_risk_summary(conn)

        conn.execute("""
            INSERT INTO audit_log (event_type, description, metadata)
            VALUES ('export', 'CSV export complete', ?)
        """, (json.dumps({"output_dir": str(OUTPUT_DIR)}),))
        conn.commit()

        print("\n[export] All exports complete.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
