"""
src/api.py — Flask REST API for the Transaction Monitoring Platform.

Endpoints:
  GET /api/stats
  GET /api/transactions           (paginated + filtered)
  GET /api/alerts                 (paginated + filtered)
  GET /api/alerts/<id>            (full alert details)
  GET /api/customers/<id>         (profile + history)
  GET /api/rules
  GET /api/analytics/timeline
  GET /api/analytics/risk-distribution
  GET /api/analytics/rule-performance
  GET /api/analytics/geographic
  GET /api/analytics/top-customers
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow imports from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

import sqlite3
from typing import Any

from flask import Flask, jsonify, request, Response

import db

app = Flask(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_conn() -> sqlite3.Connection:
    """Return a new DB connection per request."""
    return db.get_connection()


def rows_to_list(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    """Convert a list of sqlite3.Row objects to plain dicts."""
    return [dict(r) for r in rows]


def paginate(query: str, params: tuple, page: int, per_page: int, conn: sqlite3.Connection):
    """Execute `query` with pagination, returning (items, total).

    Args:
        query:    Base SQL query (no LIMIT/OFFSET).
        params:   Query parameters.
        page:     1-based page number.
        per_page: Items per page (max 200).
        conn:     Database connection.

    Returns:
        Tuple of (list[dict], total_count).
    """
    per_page = min(per_page, 200)
    offset   = (page - 1) * per_page

    # Total count
    count_q = f"SELECT COUNT(*) FROM ({query}) sub"
    (total,) = conn.execute(count_q, params).fetchone()

    # Paged results
    paged_q = f"{query} LIMIT ? OFFSET ?"
    rows = conn.execute(paged_q, params + (per_page, offset)).fetchall()
    return rows_to_list(rows), total


def error(msg: str, code: int = 400) -> Response:
    """Return a JSON error response."""
    return jsonify({"error": msg}), code


# ─────────────────────────────────────────────────────────────────────────────
# /api/stats
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/stats")
def stats():
    """Return high-level platform statistics."""
    conn = get_conn()
    try:
        data = {}

        data['customers'] = conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
        data['accounts']  = conn.execute("SELECT COUNT(*) FROM accounts").fetchone()[0]
        data['transactions'] = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]

        data['alerts'] = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
        data['open_alerts'] = conn.execute(
            "SELECT COUNT(*) FROM alerts WHERE status = 'open'"
        ).fetchone()[0]
        data['high_score_alerts'] = conn.execute(
            "SELECT COUNT(*) FROM alerts WHERE score >= 60"
        ).fetchone()[0]

        data['rules_active'] = conn.execute(
            "SELECT COUNT(*) FROM rules WHERE is_active = 1"
        ).fetchone()[0]

        data['total_volume_usd'] = conn.execute(
            "SELECT ROUND(SUM(amount_usd), 2) FROM transaction_enrichment"
        ).fetchone()[0]

        # Alert rate (% of transactions that triggered at least one alert)
        (alerted_txns,) = conn.execute(
            "SELECT COUNT(DISTINCT transaction_id) FROM alerts"
        ).fetchone()
        data['alert_rate_pct'] = round(
            100.0 * alerted_txns / data['transactions'], 2
        ) if data['transactions'] else 0

        # Risk distribution
        rows = conn.execute("""
            SELECT risk_rating, COUNT(*) n
            FROM customers GROUP BY risk_rating
        """).fetchall()
        data['customer_risk'] = {str(r['risk_rating']): r['n'] for r in rows}

        return jsonify(data)
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# /api/transactions
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/transactions")
def transactions():
    """Return paginated, filterable transaction list.

    Query params:
      page, per_page, account_id, customer_id, txn_type,
      min_amount, max_amount, start_date, end_date, alerted_only
    """
    conn = get_conn()
    try:
        page     = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 50))

        where_clauses = []
        params: list = []

        if v := request.args.get("account_id"):
            where_clauses.append("t.account_id = ?"); params.append(int(v))
        if v := request.args.get("customer_id"):
            where_clauses.append("c.id = ?"); params.append(int(v))
        if v := request.args.get("txn_type"):
            where_clauses.append("t.transaction_type = ?"); params.append(v)
        if v := request.args.get("min_amount"):
            where_clauses.append("t.amount >= ?"); params.append(float(v))
        if v := request.args.get("max_amount"):
            where_clauses.append("t.amount <= ?"); params.append(float(v))
        if v := request.args.get("start_date"):
            where_clauses.append("t.timestamp >= ?"); params.append(v)
        if v := request.args.get("end_date"):
            where_clauses.append("t.timestamp <= ?"); params.append(v)
        if request.args.get("alerted_only") == "true":
            where_clauses.append("EXISTS (SELECT 1 FROM alerts al WHERE al.transaction_id = t.id)")

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        query = f"""
            SELECT t.id, t.account_id, t.transaction_type, t.amount, t.currency,
                   t.timestamp, t.counterparty_account, t.counterparty_name,
                   t.counterparty_country, t.channel, t.status,
                   te.amount_usd, te.velocity_1h, te.velocity_24h,
                   te.country_risk_score, te.is_round_amount, te.is_large_cash,
                   c.id AS customer_id, c.name AS customer_name
            FROM transactions t
            JOIN accounts a   ON a.id = t.account_id
            JOIN customers c  ON c.id = a.customer_id
            LEFT JOIN transaction_enrichment te ON te.transaction_id = t.id
            {where_sql}
            ORDER BY t.timestamp DESC
        """
        items, total = paginate(query, tuple(params), page, per_page, conn)
        return jsonify({
            "page": page, "per_page": per_page, "total": total,
            "pages": (total + per_page - 1) // per_page,
            "data": items,
        })
    except (ValueError, sqlite3.Error) as e:
        return error(str(e))
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# /api/alerts
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/alerts")
def alerts():
    """Return paginated, filterable alert list.

    Query params:
      page, per_page, status, min_score, rule_id, category, customer_id
    """
    conn = get_conn()
    try:
        page     = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 50))

        where_clauses = []
        params: list = []

        if v := request.args.get("status"):
            where_clauses.append("al.status = ?"); params.append(v)
        if v := request.args.get("min_score"):
            where_clauses.append("al.score >= ?"); params.append(float(v))
        if v := request.args.get("rule_id"):
            where_clauses.append("al.rule_id = ?"); params.append(int(v))
        if v := request.args.get("category"):
            where_clauses.append("r.category = ?"); params.append(v)
        if v := request.args.get("customer_id"):
            where_clauses.append("c.id = ?"); params.append(int(v))

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        query = f"""
            SELECT al.id, al.transaction_id, al.rule_id, al.score, al.status,
                   al.created_date, al.resolved_date,
                   r.name AS rule_name, r.category, r.severity,
                   t.amount, t.currency, t.timestamp AS txn_timestamp,
                   t.transaction_type,
                   c.id AS customer_id, c.name AS customer_name,
                   c.risk_rating, c.pep_status
            FROM alerts al
            JOIN rules r       ON r.id = al.rule_id
            JOIN transactions t ON t.id = al.transaction_id
            JOIN accounts a    ON a.id = t.account_id
            JOIN customers c   ON c.id = a.customer_id
            {where_sql}
            ORDER BY al.score DESC, al.created_date DESC
        """
        items, total = paginate(query, tuple(params), page, per_page, conn)
        return jsonify({
            "page": page, "per_page": per_page, "total": total,
            "pages": (total + per_page - 1) // per_page,
            "data": items,
        })
    except (ValueError, sqlite3.Error) as e:
        return error(str(e))
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# /api/alerts/<id>
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/alerts/<int:alert_id>")
def alert_detail(alert_id: int):
    """Return full details for a single alert including score breakdown."""
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT al.id, al.transaction_id, al.rule_id, al.score, al.status,
                   al.created_date, al.resolved_date, al.notes,
                   r.name AS rule_name, r.description AS rule_description,
                   r.category, r.severity,
                   t.account_id, t.transaction_type, t.amount, t.currency,
                   t.timestamp, t.counterparty_account, t.counterparty_name,
                   t.counterparty_country, t.channel,
                   te.amount_usd, te.is_round_amount, te.is_large_cash,
                   te.velocity_1h, te.velocity_24h, te.velocity_7d,
                   te.amount_velocity_24h, te.country_risk_score,
                   te.is_new_counterparty, te.account_age_days,
                   c.id AS customer_id, c.name AS customer_name,
                   c.customer_type, c.risk_rating, c.country,
                   c.kyc_status, c.pep_status,
                   asc2.rule_score, asc2.customer_risk_score AS cust_risk_score,
                   asc2.geographic_risk_score, asc2.behavioral_score,
                   asc2.composite_score AS component_composite_score
            FROM alerts al
            JOIN rules r        ON r.id = al.rule_id
            JOIN transactions t ON t.id = al.transaction_id
            LEFT JOIN transaction_enrichment te ON te.transaction_id = t.id
            JOIN accounts a     ON a.id = t.account_id
            JOIN customers c    ON c.id = a.customer_id
            LEFT JOIN alert_scores asc2 ON asc2.alert_id = al.id
            WHERE al.id = ?
        """, (alert_id,)).fetchone()

        if not row:
            return error("Alert not found", 404)

        return jsonify(dict(row))
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# /api/customers/<id>
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/customers/<int:customer_id>")
def customer_profile(customer_id: int):
    """Return customer profile, accounts, recent transactions, and alert history."""
    conn = get_conn()
    try:
        cust = conn.execute("""
            SELECT c.*, cr.country_name, cr.risk_level AS country_risk_level
            FROM customers c
            JOIN country_risk cr ON cr.country_code = c.country
            WHERE c.id = ?
        """, (customer_id,)).fetchone()

        if not cust:
            return error("Customer not found", 404)

        profile = dict(cust)

        # Accounts
        accounts = conn.execute("""
            SELECT id, account_type, currency, opened_date, status, daily_limit
            FROM accounts WHERE customer_id = ?
            ORDER BY opened_date
        """, (customer_id,)).fetchall()
        profile['accounts'] = rows_to_list(accounts)

        # Recent 20 transactions
        txns = conn.execute("""
            SELECT t.id, t.account_id, t.transaction_type, t.amount, t.currency,
                   t.timestamp, t.counterparty_country, t.channel, t.status,
                   te.amount_usd, te.velocity_1h
            FROM transactions t
            JOIN accounts a ON a.id = t.account_id
            LEFT JOIN transaction_enrichment te ON te.transaction_id = t.id
            WHERE a.customer_id = ?
            ORDER BY t.timestamp DESC
            LIMIT 20
        """, (customer_id,)).fetchall()
        profile['recent_transactions'] = rows_to_list(txns)

        # Alert summary
        alert_summary = conn.execute("""
            SELECT COUNT(*) total_alerts,
                   SUM(CASE WHEN al.score >= 60 THEN 1 ELSE 0 END) high_alerts,
                   ROUND(AVG(al.score), 1) avg_score,
                   MAX(al.score) max_score
            FROM alerts al
            JOIN transactions t ON t.id = al.transaction_id
            JOIN accounts a ON a.id = t.account_id
            WHERE a.customer_id = ?
        """, (customer_id,)).fetchone()
        profile['alert_summary'] = dict(alert_summary)

        # Recent alerts
        recent_alerts = conn.execute("""
            SELECT al.id, al.score, al.status, al.created_date,
                   r.name AS rule_name, r.category
            FROM alerts al
            JOIN rules r ON r.id = al.rule_id
            JOIN transactions t ON t.id = al.transaction_id
            JOIN accounts a ON a.id = t.account_id
            WHERE a.customer_id = ?
            ORDER BY al.score DESC
            LIMIT 10
        """, (customer_id,)).fetchall()
        profile['recent_alerts'] = rows_to_list(recent_alerts)

        return jsonify(profile)
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# /api/rules
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/rules")
def rules():
    """Return all rules with current alert counts."""
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT r.id, r.name, r.description, r.category, r.severity, r.is_active,
                   COUNT(a.id) alert_count,
                   ROUND(AVG(a.score), 1) avg_score
            FROM rules r
            LEFT JOIN alerts a ON a.rule_id = r.id
            GROUP BY r.id
            ORDER BY r.category, r.name
        """).fetchall()
        return jsonify(rows_to_list(rows))
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# /api/analytics/timeline
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/analytics/timeline")
def analytics_timeline():
    """Return monthly transaction and alert counts for time-series charts."""
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT
                strftime('%Y-%m', t.timestamp) AS month,
                COUNT(t.id)                    AS transaction_count,
                ROUND(SUM(te.amount_usd), 0)   AS total_volume_usd,
                COUNT(DISTINCT al.id)           AS alert_count,
                COUNT(DISTINCT CASE WHEN al.score >= 60 THEN al.id END) AS high_alerts
            FROM transactions t
            LEFT JOIN transaction_enrichment te ON te.transaction_id = t.id
            LEFT JOIN alerts al ON al.transaction_id = t.id
            GROUP BY month
            ORDER BY month
        """).fetchall()
        return jsonify(rows_to_list(rows))
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# /api/analytics/risk-distribution
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/analytics/risk-distribution")
def analytics_risk_distribution():
    """Return customer risk distribution with transaction and alert counts."""
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT
                c.risk_rating,
                COUNT(DISTINCT c.id)  AS customers,
                COUNT(DISTINCT t.id)  AS transactions,
                COUNT(DISTINCT al.id) AS alerts,
                ROUND(AVG(al.score), 1) AS avg_alert_score
            FROM customers c
            LEFT JOIN accounts a   ON a.customer_id = c.id
            LEFT JOIN transactions t ON t.account_id = a.id
            LEFT JOIN alerts al    ON al.transaction_id = t.id
            GROUP BY c.risk_rating
            ORDER BY c.risk_rating
        """).fetchall()
        return jsonify(rows_to_list(rows))
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# /api/analytics/rule-performance
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/analytics/rule-performance")
def analytics_rule_performance():
    """Return per-rule alert counts and score statistics."""
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT
                r.id, r.name, r.category, r.severity,
                COUNT(al.id)              AS total_alerts,
                ROUND(AVG(al.score), 2)   AS avg_score,
                ROUND(MIN(al.score), 2)   AS min_score,
                ROUND(MAX(al.score), 2)   AS max_score,
                COUNT(DISTINCT t.account_id) AS unique_accounts,
                SUM(CASE WHEN al.score >= 60 THEN 1 ELSE 0 END) AS high_score_alerts
            FROM rules r
            LEFT JOIN alerts al ON al.rule_id = r.id
            LEFT JOIN transactions t ON t.id = al.transaction_id
            WHERE r.is_active = 1
            GROUP BY r.id
            ORDER BY total_alerts DESC
        """).fetchall()
        return jsonify(rows_to_list(rows))
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# /api/analytics/geographic
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/analytics/geographic")
def analytics_geographic():
    """Return transaction and alert breakdown by counterparty country."""
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT
                COALESCE(t.counterparty_country, 'DOMESTIC') AS country_code,
                cr.country_name,
                cr.risk_level,
                COUNT(t.id)               AS transaction_count,
                ROUND(SUM(te.amount_usd), 0) AS total_usd,
                COUNT(DISTINCT al.id)     AS alert_count
            FROM transactions t
            LEFT JOIN country_risk cr ON cr.country_code = t.counterparty_country
            LEFT JOIN transaction_enrichment te ON te.transaction_id = t.id
            LEFT JOIN alerts al ON al.transaction_id = t.id
            GROUP BY t.counterparty_country
            ORDER BY alert_count DESC
            LIMIT 40
        """).fetchall()
        return jsonify(rows_to_list(rows))
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# /api/analytics/top-customers
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/analytics/top-customers")
def analytics_top_customers():
    """Return the top 20 riskiest customers by alert score and count."""
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT
                c.id, c.name, c.customer_type, c.risk_rating,
                c.country, c.pep_status, c.kyc_status,
                COUNT(DISTINCT al.id)   AS total_alerts,
                ROUND(MAX(al.score), 1) AS max_score,
                ROUND(AVG(al.score), 1) AS avg_score,
                COUNT(DISTINCT t.id)    AS total_transactions,
                ROUND(SUM(te.amount_usd), 0) AS total_volume_usd
            FROM customers c
            JOIN accounts a    ON a.customer_id = c.id
            JOIN transactions t ON t.account_id = a.id
            LEFT JOIN transaction_enrichment te ON te.transaction_id = t.id
            LEFT JOIN alerts al ON al.transaction_id = t.id
            GROUP BY c.id
            HAVING total_alerts > 0
            ORDER BY max_score DESC, total_alerts DESC
            LIMIT 20
        """).fetchall()
        return jsonify(rows_to_list(rows))
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Starting Transaction Monitor API on http://127.0.0.1:5000")
    app.run(debug=False, port=5000)
