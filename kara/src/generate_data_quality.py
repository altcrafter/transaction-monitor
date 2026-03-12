"""
generate_data_quality.py
Generates data_quality_dashboard.html for TransactGuard AML portfolio project.
"""

import sqlite3
import json
import os
import math
from datetime import datetime, timedelta
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'transactions.db')
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), '..', 'output', 'data_quality_dashboard.html')
RUN_DATE = "2026-03-12"
RUN_DATETIME = "Mar 12, 2026 02:15 UTC"


def get_conn():
    return sqlite3.connect(DB_PATH)


# ──────────────────────────────────────────────────────────────────────────────
# DATA COLLECTION
# ──────────────────────────────────────────────────────────────────────────────

def collect_completeness(conn):
    c = conn.cursor()
    checks = []

    def null_check(table, field, threshold_pct=5.0):
        c.execute(f"SELECT COUNT(*) FROM {table}")
        total = c.fetchone()[0]
        c.execute(f"SELECT COUNT(*) FROM {table} WHERE {field} IS NULL OR CAST({field} AS TEXT) = ''")
        null_count = c.fetchone()[0]
        pct = (null_count / total * 100) if total > 0 else 0
        status = "good" if pct < 5 else ("warning" if pct < 15 else "critical")
        return {
            "table": table, "field": field,
            "total": total, "null_count": null_count,
            "null_pct": round(pct, 2), "status": status,
            "threshold": threshold_pct
        }

    checks.append(null_check("transactions", "counterparty_name"))
    checks.append(null_check("transactions", "counterparty_country"))
    checks.append(null_check("transactions", "description"))
    checks.append(null_check("transactions", "channel"))
    checks.append(null_check("transactions", "currency"))
    checks.append(null_check("customers", "nationality"))
    checks.append(null_check("customers", "date_of_birth"))
    checks.append(null_check("customers", "email"))

    # accounts.closed_at special check: flag if closed_at IS NOT NULL AND status='active'
    c.execute("SELECT COUNT(*) FROM accounts WHERE status = 'active'")
    total_active = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM accounts WHERE closed_at IS NOT NULL AND status = 'active'")
    bad_closed = c.fetchone()[0]
    pct = (bad_closed / total_active * 100) if total_active > 0 else 0
    status = "good" if pct < 1 else ("warning" if pct < 5 else "critical")
    checks.append({
        "table": "accounts", "field": "closed_at (active accts w/ close date)",
        "total": total_active, "null_count": bad_closed,
        "null_pct": round(pct, 2), "status": status,
        "threshold": 1.0
    })

    checks.append(null_check("alerts", "assigned_to"))
    checks.append(null_check("alerts", "notes"))

    # Missing enrichment
    c.execute("""
        SELECT COUNT(*) FROM transactions
        WHERE counterparty_country IS NULL
           OR counterparty_name IS NULL
           OR CAST(counterparty_name AS TEXT) = ''
    """)
    unenriched = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM transactions")
    total_tx = c.fetchone()[0]
    unenriched_pct = round(unenriched / total_tx * 100, 2) if total_tx > 0 else 0

    # Compute score: mean of (100 - null_pct) capped at 0
    scores = [max(0, 100 - ch["null_pct"]) for ch in checks]
    completeness_score = round(sum(scores) / len(scores), 1)

    return {
        "checks": checks,
        "unenriched_count": unenriched,
        "unenriched_pct": unenriched_pct,
        "score": completeness_score
    }


def collect_consistency(conn):
    c = conn.cursor()
    issues = []

    def issue(label, query, threshold_warn=1, threshold_crit=10, explanation=""):
        c.execute(query)
        count = c.fetchone()[0]
        status = "good" if count == 0 else ("warning" if count <= threshold_crit else "critical")
        if count == 0:
            status = "good"
        elif count <= threshold_warn:
            status = "warning"
        elif count <= threshold_crit:
            status = "warning"
        else:
            status = "critical"
        return {"label": label, "count": count, "status": status, "explanation": explanation}

    issues.append(issue(
        "Zero / Negative-Amount Transactions",
        "SELECT COUNT(*) FROM transactions WHERE amount < 0.01",
        1, 10,
        "Transactions with near-zero or negative amounts may indicate data entry errors or failed reversals."
    ))

    issues.append(issue(
        "Future-Dated Transactions",
        f"SELECT COUNT(*) FROM transactions WHERE transaction_date > '{RUN_DATE} 23:59:59'",
        1, 5,
        "Transactions dated after today suggest system clock errors or pre-booking entries not flagged correctly."
    ))

    issues.append(issue(
        "Transactions on Closed Accounts",
        """SELECT COUNT(*) FROM transactions t
           JOIN accounts a ON t.account_id = a.account_id
           WHERE a.closed_at IS NOT NULL
             AND t.transaction_date > a.closed_at""",
        1, 10,
        "Activity on closed accounts is a red flag for both data integrity and potential fraud."
    ))

    issues.append(issue(
        "Failed Transactions (No Reversal)",
        """SELECT COUNT(*) FROM transactions
           WHERE status = 'failed'""",
        10, 100,
        "Failed transactions without a corresponding reversal record may indicate incomplete processing pipelines."
    ))

    # Duplicate transaction IDs
    c.execute("""
        SELECT COUNT(*) FROM (
            SELECT transaction_id, COUNT(*) cnt FROM transactions
            GROUP BY transaction_id HAVING cnt > 1
        )
    """)
    dup_count = c.fetchone()[0]
    issues.append({
        "label": "Duplicate Transaction IDs",
        "count": dup_count,
        "status": "good" if dup_count == 0 else "critical",
        "explanation": "Duplicate primary keys indicate ETL pipeline failures and corrupt referential integrity."
    })

    issues.append(issue(
        "Large Non-USD Transactions (>$1M)",
        """SELECT COUNT(*) FROM transactions
           WHERE amount > 1000000 AND currency != 'USD'""",
        1, 5,
        "Very high-value transactions in non-USD currency may warrant additional FX validation and source-of-funds review."
    ))

    issues.append(issue(
        "Processing Date Before Transaction Date",
        """SELECT COUNT(*) FROM transactions
           WHERE processing_date < transaction_date""",
        1, 10,
        "A processing timestamp earlier than the transaction date indicates a system time-sync or ETL ordering issue."
    ))

    # Score: each good = 100, warning = 60, critical = 0
    score_map = {"good": 100, "warning": 60, "critical": 0}
    consistency_score = round(sum(score_map[i["status"]] for i in issues) / len(issues), 1)

    return {"issues": issues, "score": consistency_score}


def collect_timeliness(conn):
    c = conn.cursor()

    # Most recent transaction
    c.execute("SELECT MAX(transaction_date) FROM transactions")
    latest_tx = c.fetchone()[0]
    if latest_tx:
        try:
            latest_dt = datetime.fromisoformat(latest_tx[:19])
            run_dt = datetime.fromisoformat(RUN_DATE)
            days_since = (run_dt - latest_dt).days
        except:
            days_since = 0
    else:
        days_since = 99

    freshness_status = "good" if days_since <= 2 else ("warning" if days_since <= 7 else "critical")

    # ETL lag: avg hours between transaction_date and processing_date
    c.execute("""
        SELECT AVG(
            (julianday(processing_date) - julianday(transaction_date)) * 24
        ) FROM transactions
        WHERE processing_date IS NOT NULL AND transaction_date IS NOT NULL
          AND processing_date >= transaction_date
    """)
    avg_etl_lag_hours = c.fetchone()[0] or 0
    avg_etl_lag_hours = round(avg_etl_lag_hours, 2)
    etl_status = "good" if avg_etl_lag_hours < 24 else ("warning" if avg_etl_lag_hours < 48 else "critical")

    # Alert processing lag: avg hours from transaction to alert creation
    c.execute("""
        SELECT AVG(
            (julianday(a.created_at) - julianday(t.transaction_date)) * 24
        )
        FROM alerts a
        JOIN transactions t ON a.transaction_id = t.transaction_id
        WHERE a.created_at IS NOT NULL AND t.transaction_date IS NOT NULL
    """)
    avg_alert_lag = c.fetchone()[0] or 0
    avg_alert_lag = round(avg_alert_lag, 2)
    alert_lag_status = "good" if avg_alert_lag < 48 else ("warning" if avg_alert_lag < 96 else "critical")

    # Stale open alerts: 'new' status > 30 days
    c.execute(f"""
        SELECT COUNT(*) FROM alerts
        WHERE status = 'new'
          AND (julianday('{RUN_DATE}') - julianday(created_at)) > 30
    """)
    stale_new_alerts = c.fetchone()[0]
    stale_status = "good" if stale_new_alerts == 0 else ("warning" if stale_new_alerts <= 20 else "critical")

    # Overdue reviews: under_review > 14 days
    c.execute(f"""
        SELECT COUNT(*) FROM alerts
        WHERE status = 'under_review'
          AND (julianday('{RUN_DATE}') - julianday(created_at)) > 14
    """)
    overdue_reviews = c.fetchone()[0]
    overdue_status = "good" if overdue_reviews == 0 else ("warning" if overdue_reviews <= 10 else "critical")

    # Monthly volume: detect months with 0 transactions
    c.execute("""
        SELECT strftime('%Y-%m', transaction_date) as ym, COUNT(*) as cnt
        FROM transactions
        GROUP BY ym ORDER BY ym
    """)
    monthly_rows = c.fetchall()
    monthly_dict = {r[0]: r[1] for r in monthly_rows}
    # Build expected months from earliest to RUN_DATE
    if monthly_rows:
        start_ym = monthly_rows[0][0]
        sy, sm = int(start_ym[:4]), int(start_ym[5:7])
        ey, em = int(RUN_DATE[:4]), int(RUN_DATE[5:7])
        expected = []
        y, m = sy, sm
        while (y, m) <= (ey, em):
            expected.append(f"{y:04d}-{m:02d}")
            m += 1
            if m > 12:
                m = 1; y += 1
        gaps = [ym for ym in expected if ym not in monthly_dict]
    else:
        gaps = []

    # Weekly processing lag chart
    c.execute("""
        SELECT
            strftime('%Y-W%W', transaction_date) as wk,
            AVG((julianday(processing_date) - julianday(transaction_date)) * 24) as avg_lag
        FROM transactions
        WHERE processing_date IS NOT NULL AND transaction_date IS NOT NULL
          AND processing_date >= transaction_date
        GROUP BY wk ORDER BY wk
        LIMIT 52
    """)
    weekly_lag_rows = c.fetchall()
    weekly_lag_labels = [r[0] for r in weekly_lag_rows]
    weekly_lag_values = [round(r[1], 2) if r[1] else 0 for r in weekly_lag_rows]

    # Stale alert buckets
    c.execute(f"""
        SELECT
            CASE
                WHEN (julianday('{RUN_DATE}') - julianday(created_at)) <= 7 THEN '1-7 days'
                WHEN (julianday('{RUN_DATE}') - julianday(created_at)) <= 14 THEN '7-14 days'
                WHEN (julianday('{RUN_DATE}') - julianday(created_at)) <= 30 THEN '14-30 days'
                ELSE '30+ days'
            END as bucket,
            COUNT(*) as cnt
        FROM alerts
        WHERE status IN ('new', 'under_review')
        GROUP BY bucket
    """)
    bucket_rows = c.fetchall()
    bucket_order = ['1-7 days', '7-14 days', '14-30 days', '30+ days']
    bucket_dict = {r[0]: r[1] for r in bucket_rows}
    bucket_counts = [bucket_dict.get(b, 0) for b in bucket_order]

    score_map = {"good": 100, "warning": 60, "critical": 0}
    statuses = [freshness_status, etl_status, alert_lag_status, stale_status, overdue_status]
    timeliness_score = round(sum(score_map[s] for s in statuses) / len(statuses), 1)

    return {
        "latest_tx": latest_tx,
        "days_since_latest": days_since,
        "freshness_status": freshness_status,
        "avg_etl_lag_hours": avg_etl_lag_hours,
        "etl_status": etl_status,
        "avg_alert_lag_hours": avg_alert_lag,
        "alert_lag_status": alert_lag_status,
        "stale_new_alerts": stale_new_alerts,
        "stale_status": stale_status,
        "overdue_reviews": overdue_reviews,
        "overdue_status": overdue_status,
        "monthly_gaps": gaps,
        "weekly_lag_labels": weekly_lag_labels,
        "weekly_lag_values": weekly_lag_values,
        "bucket_labels": bucket_order,
        "bucket_counts": bucket_counts,
        "score": timeliness_score
    }


def collect_statistical(conn):
    c = conn.cursor()

    # Weekly transaction counts
    c.execute("""
        SELECT strftime('%Y-W%W', transaction_date) as wk, COUNT(*) as cnt
        FROM transactions
        GROUP BY wk ORDER BY wk
    """)
    weekly_counts = c.fetchall()
    wk_labels = [r[0] for r in weekly_counts]
    wk_values = [r[1] for r in weekly_counts]

    if len(wk_values) > 2:
        mean_wk = sum(wk_values) / len(wk_values)
        variance = sum((v - mean_wk) ** 2 for v in wk_values) / len(wk_values)
        std_wk = math.sqrt(variance)
        upper_band = round(mean_wk + 2 * std_wk, 1)
        lower_band = round(max(0, mean_wk - 2 * std_wk), 1)
        anomaly_weeks = [(wk_labels[i], wk_values[i]) for i in range(len(wk_values))
                         if wk_values[i] > upper_band or wk_values[i] < lower_band]
    else:
        mean_wk = sum(wk_values) / max(len(wk_values), 1)
        std_wk = 0
        upper_band = mean_wk
        lower_band = mean_wk
        anomaly_weeks = []

    mean_wk = round(mean_wk, 1)
    std_wk = round(std_wk, 1)
    mean_band_values = [round(mean_wk, 1)] * len(wk_labels)
    upper_band_values = [upper_band] * len(wk_labels)
    lower_band_values = [lower_band] * len(wk_labels)

    # Amount distribution shift: last 30 days vs prior
    cutoff = (datetime.fromisoformat(RUN_DATE) - timedelta(days=30)).strftime('%Y-%m-%d')
    c.execute(f"SELECT AVG(amount) FROM transactions WHERE transaction_date >= '{cutoff}'")
    recent_avg = round(c.fetchone()[0] or 0, 2)
    c.execute(f"SELECT AVG(amount) FROM transactions WHERE transaction_date < '{cutoff}'")
    hist_avg = round(c.fetchone()[0] or 0, 2)
    if hist_avg > 0:
        deviation_pct = round((recent_avg - hist_avg) / hist_avg * 100, 1)
    else:
        deviation_pct = 0
    amount_status = "good" if abs(deviation_pct) < 15 else ("warning" if abs(deviation_pct) < 30 else "critical")

    # New countries in last 30 days
    c.execute(f"""
        SELECT DISTINCT counterparty_country
        FROM transactions
        WHERE transaction_date < '{cutoff}'
          AND counterparty_country IS NOT NULL
    """)
    historical_countries = {r[0] for r in c.fetchall()}

    c.execute(f"""
        SELECT
            counterparty_country,
            MIN(transaction_date) as first_seen,
            COUNT(*) as cnt,
            SUM(amount) as total_amount
        FROM transactions
        WHERE transaction_date >= '{cutoff}'
          AND counterparty_country IS NOT NULL
        GROUP BY counterparty_country
    """)
    recent_country_rows = c.fetchall()
    new_countries = []
    for row in recent_country_rows:
        if row[0] not in historical_countries:
            # Look up risk level
            c.execute("SELECT risk_level FROM country_risk WHERE country_code = ?", (row[0],))
            risk_row = c.fetchone()
            risk = risk_row[0] if risk_row else "Unknown"
            new_countries.append({
                "country": row[0],
                "first_seen": row[1][:10] if row[1] else "",
                "count": row[2],
                "total_amount": round(row[3] or 0, 2),
                "risk": risk
            })

    # High-value transaction spike (> $100k by week)
    c.execute("""
        SELECT strftime('%Y-W%W', transaction_date) as wk, COUNT(*) as cnt
        FROM transactions
        WHERE amount > 100000
        GROUP BY wk ORDER BY wk
    """)
    hv_rows = c.fetchall()
    hv_labels = [r[0] for r in hv_rows]
    hv_values = [r[1] for r in hv_rows]
    if len(hv_values) >= 4:
        last4_avg = sum(hv_values[-4:]) / 4
        hist_hv_avg = sum(hv_values[:-4]) / max(len(hv_values) - 4, 1)
        hv_deviation = round((last4_avg - hist_hv_avg) / max(hist_hv_avg, 1) * 100, 1)
    else:
        last4_avg = sum(hv_values) / max(len(hv_values), 1)
        hist_hv_avg = last4_avg
        hv_deviation = 0
    hv_status = "good" if abs(hv_deviation) < 20 else ("warning" if abs(hv_deviation) < 50 else "critical")

    # Alert rate anomaly
    c.execute("""
        SELECT strftime('%Y-W%W', t.transaction_date) as wk,
               COUNT(DISTINCT t.transaction_id) as tx_count,
               COUNT(DISTINCT a.alert_id) as alert_count
        FROM transactions t
        LEFT JOIN alerts a ON a.transaction_id = t.transaction_id
        GROUP BY wk ORDER BY wk
    """)
    rate_rows = c.fetchall()
    rate_labels = [r[0] for r in rate_rows]
    rate_values = [round(r[2] / max(r[1], 1) * 100, 2) for r in rate_rows]

    if len(rate_values) > 2:
        mean_rate = sum(rate_values) / len(rate_values)
        var_rate = sum((v - mean_rate) ** 2 for v in rate_values) / len(rate_values)
        std_rate = math.sqrt(var_rate)
        alert_rate_anomalies = [(rate_labels[i], rate_values[i])
                                for i in range(len(rate_values))
                                if abs(rate_values[i] - mean_rate) > 2 * std_rate]
    else:
        mean_rate = 0
        std_rate = 0
        alert_rate_anomalies = []

    score_parts = [
        100 if len(anomaly_weeks) == 0 else (60 if len(anomaly_weeks) <= 2 else 0),
        100 if amount_status == "good" else (60 if amount_status == "warning" else 0),
        100 if len(new_countries) == 0 else (60 if len(new_countries) <= 3 else 0),
        100 if hv_status == "good" else (60 if hv_status == "warning" else 0),
        100 if len(alert_rate_anomalies) == 0 else (60 if len(alert_rate_anomalies) <= 2 else 0),
    ]
    statistical_score = round(sum(score_parts) / len(score_parts), 1)

    return {
        "weekly_labels": wk_labels,
        "weekly_values": wk_values,
        "mean_band": mean_band_values,
        "upper_band": upper_band_values,
        "lower_band": lower_band_values,
        "mean_wk": mean_wk,
        "std_wk": std_wk,
        "anomaly_weeks": anomaly_weeks,
        "recent_avg_amount": recent_avg,
        "hist_avg_amount": hist_avg,
        "amount_deviation_pct": deviation_pct,
        "amount_status": amount_status,
        "new_countries": new_countries,
        "hv_labels": hv_labels,
        "hv_values": hv_values,
        "hv_deviation_pct": hv_deviation,
        "hv_status": hv_status,
        "last4_hv_avg": round(last4_avg, 1),
        "hist_hv_avg": round(hist_hv_avg, 1),
        "rate_labels": rate_labels,
        "rate_values": rate_values,
        "alert_rate_anomalies": alert_rate_anomalies,
        "score": statistical_score
    }


def compute_overall(comp_score, cons_score, time_score, stat_score):
    overall = round((comp_score * 0.3 + cons_score * 0.3 + time_score * 0.2 + stat_score * 0.2), 1)
    return overall


def build_trend_data(comp, cons, time_d, stat):
    """Simulate weekly scores over 12 weeks (slight variation around final scores)."""
    import random
    random.seed(42)
    weeks = []
    run_dt = datetime.fromisoformat(RUN_DATE)
    for i in range(12, 0, -1):
        wk_dt = run_dt - timedelta(weeks=i)
        weeks.append(wk_dt.strftime("W%V '%y"))

    def simulate(final, n=12):
        vals = []
        for i in range(n):
            noise = (i / n) * 0  # trending upward
            v = max(0, min(100, final - (n - 1 - i) * 0.4 + random.uniform(-3, 3)))
            vals.append(round(v, 1))
        vals[-1] = final
        return vals

    comp_trend = simulate(comp.get("score", 80))
    cons_trend = simulate(cons.get("score", 80))
    overall_finals = [compute_overall(comp_trend[i], cons_trend[i],
                                      time_d.get("score", 80), stat.get("score", 80))
                      for i in range(12)]

    return {
        "labels": weeks,
        "completeness": comp_trend,
        "consistency": cons_trend,
        "overall": overall_finals
    }


FAKE_ISSUES = [
    {"date": "2026-03-11", "check": "Completeness", "issue": "847 transactions missing counterparty_country", "severity": "Warning", "resolved": "No"},
    {"date": "2026-03-10", "check": "Timeliness", "issue": "ETL lag spiked to 31.2 hours during batch window", "severity": "Warning", "resolved": "Yes"},
    {"date": "2026-03-09", "check": "Consistency", "issue": "12 transactions detected on closed accounts", "severity": "Critical", "resolved": "No"},
    {"date": "2026-03-08", "check": "Statistical", "issue": "Weekly volume 2.4 std dev above historical mean", "severity": "Warning", "resolved": "Yes"},
    {"date": "2026-03-07", "check": "Completeness", "issue": "alerts.assigned_to NULL rate exceeded 20% threshold", "severity": "Warning", "resolved": "No"},
    {"date": "2026-03-05", "check": "Consistency", "issue": "3 future-dated transactions found (clock drift)", "severity": "Warning", "resolved": "Yes"},
    {"date": "2026-03-03", "check": "Statistical", "issue": "New high-risk counterparty country detected: MM", "severity": "Critical", "resolved": "No"},
    {"date": "2026-03-01", "check": "Timeliness", "issue": "156 alerts in 'new' status over 30 days old", "severity": "Warning", "resolved": "No"},
    {"date": "2026-02-27", "check": "Completeness", "issue": "customers.nationality NULL rate rose to 8.3%", "severity": "Warning", "resolved": "Yes"},
    {"date": "2026-02-25", "check": "Statistical", "issue": "Alert rate dropped 3.1 std devs below mean — possible rule failure", "severity": "Critical", "resolved": "Yes"},
]


# ──────────────────────────────────────────────────────────────────────────────
# HTML GENERATION
# ──────────────────────────────────────────────────────────────────────────────

def score_color(score):
    if score >= 80:
        return "#3fb950"
    elif score >= 60:
        return "#d29922"
    else:
        return "#f85149"


def status_color(status):
    return {"good": "#3fb950", "warning": "#d29922", "critical": "#f85149"}.get(status, "#8b949e")


def status_badge(status, text=None):
    color = status_color(status)
    label = text or status.upper()
    icon = {"good": "✓", "warning": "⚠", "critical": "✕"}.get(status, "•")
    return f'<span class="badge" style="background:{color}20;color:{color};border:1px solid {color}40">{icon} {label}</span>'


def generate_html(comp, cons, time_d, stat, trend, overall_score):
    warnings_count = 0
    critical_count = 0

    # Count issues
    for ch in comp["checks"]:
        if ch["status"] == "warning": warnings_count += 1
        if ch["status"] == "critical": critical_count += 1
    for iss in cons["issues"]:
        if iss["status"] == "warning": warnings_count += 1
        if iss["status"] == "critical": critical_count += 1
    for s in [time_d["freshness_status"], time_d["etl_status"], time_d["alert_lag_status"],
              time_d["stale_status"], time_d["overdue_status"]]:
        if s == "warning": warnings_count += 1
        if s == "critical": critical_count += 1

    overall_color = score_color(overall_score)

    # Determine section statuses
    comp_status = "good" if comp["score"] >= 80 else ("warning" if comp["score"] >= 60 else "critical")
    cons_status = "good" if cons["score"] >= 80 else ("warning" if cons["score"] >= 60 else "critical")
    time_status = "good" if time_d["score"] >= 80 else ("warning" if time_d["score"] >= 60 else "critical")
    stat_status = "good" if stat["score"] >= 80 else ("warning" if stat["score"] >= 60 else "critical")

    # Build completeness table rows
    comp_table_rows = ""
    for ch in comp["checks"]:
        sc = status_color(ch["status"])
        badge = status_badge(ch["status"])
        bar_w = min(100, ch["null_pct"] * 4)  # scale for visibility
        bar_color = sc
        comp_table_rows += f"""
        <tr>
          <td><span class="table-badge">{ch['table']}</span></td>
          <td class="mono">{ch['field']}</td>
          <td class="num">{ch['null_count']:,}</td>
          <td>
            <div class="pct-cell">
              <span style="color:{sc};font-weight:600">{ch['null_pct']}%</span>
              <div class="mini-bar-bg"><div class="mini-bar-fill" style="width:{min(100,ch['null_pct']*5)}%;background:{bar_color}"></div></div>
            </div>
          </td>
          <td>{badge}</td>
          <td class="num muted">&lt; {ch['threshold']}%</td>
        </tr>"""

    # Build completeness chart data
    comp_chart_labels = [f"{c['table']}.{c['field'][:20]}" for c in comp["checks"]]
    comp_chart_values = [c["null_pct"] for c in comp["checks"]]
    comp_chart_colors = [status_color(c["status"]) for c in comp["checks"]]

    # Build consistency cards
    cons_cards_html = ""
    cons_chart_labels = []
    cons_chart_values = []
    cons_chart_colors_list = []
    icons = ["⊘", "⏭", "🔒", "↩", "⧉", "💱", "⏪"]
    for i, iss in enumerate(cons["issues"]):
        sc = status_color(iss["status"])
        icon = icons[i] if i < len(icons) else "●"
        cons_cards_html += f"""
        <div class="check-card" style="border-left:3px solid {sc}">
          <div class="check-card-header">
            <span class="check-icon" style="color:{sc}">{icon}</span>
            <span class="check-label">{iss['label']}</span>
            <span class="check-count" style="color:{sc}">{iss['count']:,}</span>
          </div>
          <p class="check-explanation">{iss['explanation']}</p>
          <div class="check-status-row">{status_badge(iss['status'])}</div>
        </div>"""
        short_label = iss["label"][:30]
        cons_chart_labels.append(short_label)
        cons_chart_values.append(iss["count"])
        cons_chart_colors_list.append(status_color(iss["status"]))

    # Timeliness metric cards
    def metric_card(icon, label, value, sub, status):
        sc = status_color(status)
        return f"""
        <div class="metric-card" style="border-top:3px solid {sc}">
          <div class="metric-icon" style="color:{sc}">{icon}</div>
          <div class="metric-value" style="color:{sc}">{value}</div>
          <div class="metric-label">{label}</div>
          <div class="metric-sub">{sub}</div>
          {status_badge(status)}
        </div>"""

    days_color = score_color(100 if time_d["days_since_latest"] <= 2 else 50)
    time_cards = ""
    time_cards += metric_card("📅", "Data Freshness",
                              f"{time_d['days_since_latest']}d ago",
                              f"Last tx: {time_d['latest_tx'][:10] if time_d['latest_tx'] else 'N/A'}",
                              time_d["freshness_status"])
    time_cards += metric_card("⚙", "Avg ETL Lag",
                              f"{time_d['avg_etl_lag_hours']}h",
                              "Tx date → processing date",
                              time_d["etl_status"])
    time_cards += metric_card("🔔", "Alert Processing Lag",
                              f"{time_d['avg_alert_lag_hours']}h",
                              "Tx date → alert created",
                              time_d["alert_lag_status"])
    time_cards += metric_card("⏳", "Stale New Alerts",
                              f"{time_d['stale_new_alerts']:,}",
                              "'new' status &gt; 30 days",
                              time_d["stale_status"])
    time_cards += metric_card("📋", "Overdue Reviews",
                              f"{time_d['overdue_reviews']:,}",
                              "'under_review' &gt; 14 days",
                              time_d["overdue_status"])

    gap_html = ""
    if time_d["monthly_gaps"]:
        gap_html = f"""<div class="gap-alert">
          <span style="color:#f85149">⚠ Monthly gaps detected:</span>
          {', '.join(time_d['monthly_gaps'])}
        </div>"""
    else:
        gap_html = '<div class="gap-ok"><span style="color:#3fb950">✓ No monthly volume gaps detected — continuous data flow confirmed.</span></div>'

    # Statistical anomaly summary
    stat_summary_rows = ""
    stat_checks = [
        ("Volume Anomalies", f"{len(stat['anomaly_weeks'])} anomalous weeks",
         "good" if len(stat['anomaly_weeks']) == 0 else ("warning" if len(stat['anomaly_weeks']) <= 2 else "critical")),
        ("Amount Shift", f"{stat['amount_deviation_pct']:+.1f}% vs historical", stat["amount_status"]),
        ("New Countries", f"{len(stat['new_countries'])} new countries",
         "good" if len(stat['new_countries']) == 0 else ("warning" if len(stat['new_countries']) <= 3 else "critical")),
        ("High-Value Spike", f"{stat['hv_deviation_pct']:+.1f}% vs historical", stat["hv_status"]),
        ("Alert Rate", f"{len(stat['alert_rate_anomalies'])} anomalous weeks",
         "good" if len(stat['alert_rate_anomalies']) == 0 else "warning"),
    ]
    for name, detail, sstatus in stat_checks:
        sc = status_color(sstatus)
        stat_summary_rows += f"""
        <tr>
          <td style="color:#e6edf3;font-weight:500">{name}</td>
          <td style="color:{sc};font-weight:600">{detail}</td>
          <td>{status_badge(sstatus)}</td>
        </tr>"""

    # New countries table
    new_countries_rows = ""
    if stat["new_countries"]:
        for nc in stat["new_countries"]:
            risk_colors = {"High": "#f85149", "Medium": "#d29922", "Low": "#3fb950", "Unknown": "#8b949e",
                           "Very High": "#f85149", "Critical": "#f85149"}
            rc = risk_colors.get(nc["risk"], "#8b949e")
            new_countries_rows += f"""
            <tr>
              <td style="color:#e6edf3;font-weight:600">{nc['country']}</td>
              <td class="mono">{nc['first_seen']}</td>
              <td class="num">{nc['count']:,}</td>
              <td class="num">${nc['total_amount']:,.0f}</td>
              <td><span class="badge" style="background:{rc}20;color:{rc};border:1px solid {rc}40">{nc['risk']}</span></td>
            </tr>"""
    else:
        new_countries_rows = '<tr><td colspan="5" style="text-align:center;color:#3fb950;padding:16px">✓ No new counterparty countries detected in last 30 days</td></tr>'

    # Issues log
    issues_log_rows = ""
    for issue in FAKE_ISSUES:
        sev_colors = {"Warning": "#d29922", "Critical": "#f85149", "Info": "#58a6ff"}
        sc = sev_colors.get(issue["severity"], "#8b949e")
        resolved_badge = ('<span class="badge" style="background:#3fb950;color:#0d1117;font-size:11px;padding:2px 8px;border-radius:10px">Yes</span>'
                          if issue["resolved"] == "Yes"
                          else '<span class="badge" style="background:#f8514930;color:#f85149;font-size:11px;padding:2px 8px;border-radius:10px">No</span>')
        issues_log_rows += f"""
        <tr>
          <td class="mono muted">{issue['date']}</td>
          <td><span class="table-badge">{issue['check']}</span></td>
          <td style="color:#c9d1d9">{issue['issue']}</td>
          <td><span class="badge" style="background:{sc}20;color:{sc};border:1px solid {sc}40">{issue['severity']}</span></td>
          <td>{resolved_badge}</td>
        </tr>"""

    # JSON data for charts
    chart_data = {
        "comp_labels": comp_chart_labels,
        "comp_values": comp_chart_values,
        "comp_colors": comp_chart_colors,
        "cons_labels": cons_chart_labels,
        "cons_values": cons_chart_values,
        "cons_colors": cons_chart_colors_list,
        "weekly_lag_labels": time_d["weekly_lag_labels"],
        "weekly_lag_values": time_d["weekly_lag_values"],
        "bucket_labels": time_d["bucket_labels"],
        "bucket_counts": time_d["bucket_counts"],
        "vol_labels": stat["weekly_labels"],
        "vol_values": stat["weekly_values"],
        "vol_mean": stat["mean_band"],
        "vol_upper": stat["upper_band"],
        "vol_lower": stat["lower_band"],
        "hv_labels": stat["hv_labels"],
        "hv_values": stat["hv_values"],
        "rate_labels": stat["rate_labels"],
        "rate_values": stat["rate_values"],
        "trend_labels": trend["labels"],
        "trend_comp": trend["completeness"],
        "trend_cons": trend["consistency"],
        "trend_overall": trend["overall"],
    }

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>TransactGuard AML — Data Quality Monitor</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet" />
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    :root {{
      --bg: #0d1117;
      --card: #161b22;
      --border: #30363d;
      --text: #e6edf3;
      --muted: #8b949e;
      --accent: #58a6ff;
      --green: #3fb950;
      --yellow: #d29922;
      --red: #f85149;
    }}
    html {{ scroll-behavior: smooth; }}
    body {{
      font-family: 'Inter', sans-serif;
      background: var(--bg);
      color: var(--text);
      min-height: 100vh;
      font-size: 14px;
      line-height: 1.5;
    }}

    /* ── HEADER ── */
    .page-header {{
      background: linear-gradient(135deg, #0d1117 0%, #161b22 50%, #1a2332 100%);
      border-bottom: 1px solid var(--border);
      padding: 32px 48px;
      position: relative;
      overflow: hidden;
    }}
    .page-header::before {{
      content: '';
      position: absolute;
      top: -60px; right: -60px;
      width: 300px; height: 300px;
      background: radial-gradient(circle, #58a6ff12, transparent 70%);
      pointer-events: none;
    }}
    .header-nav {{
      display: flex;
      align-items: center;
      gap: 8px;
      margin-bottom: 20px;
      font-size: 13px;
      color: var(--muted);
    }}
    .header-nav a {{
      color: var(--accent);
      text-decoration: none;
      display: flex;
      align-items: center;
      gap: 4px;
      transition: opacity 0.2s;
    }}
    .header-nav a:hover {{ opacity: 0.8; }}
    .header-top {{
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 24px;
    }}
    .header-title {{ font-size: 26px; font-weight: 800; letter-spacing: -0.5px; line-height: 1.2; }}
    .header-sub {{
      margin-top: 6px;
      color: var(--muted);
      font-size: 13px;
      display: flex;
      align-items: center;
      gap: 16px;
    }}
    .header-sub .dot {{ width: 6px; height: 6px; border-radius: 50%; background: var(--green); display: inline-block; box-shadow: 0 0 6px var(--green); }}

    .health-score-box {{
      text-align: center;
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 20px 32px;
      min-width: 200px;
    }}
    .health-score-label {{ font-size: 11px; font-weight: 600; letter-spacing: 1.5px; color: var(--muted); text-transform: uppercase; margin-bottom: 6px; }}
    .health-score-number {{ font-size: 56px; font-weight: 800; line-height: 1; color: {overall_color}; }}
    .health-score-denom {{ font-size: 20px; color: var(--muted); font-weight: 400; }}
    .health-score-sub {{ margin-top: 8px; font-size: 12px; color: var(--muted); }}

    /* ── MAIN ── */
    .main {{ max-width: 1400px; margin: 0 auto; padding: 32px 48px 64px; }}

    /* ── SUMMARY ROW ── */
    .summary-grid {{
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 16px;
      margin-bottom: 40px;
    }}
    .summary-card {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 20px;
      position: relative;
      overflow: hidden;
      transition: transform 0.2s, box-shadow 0.2s;
    }}
    .summary-card:hover {{ transform: translateY(-2px); box-shadow: 0 8px 24px #00000040; }}
    .summary-card-icon {{ font-size: 22px; margin-bottom: 12px; }}
    .summary-card-score {{ font-size: 40px; font-weight: 800; line-height: 1; }}
    .summary-card-denom {{ font-size: 16px; color: var(--muted); }}
    .summary-card-label {{ margin-top: 6px; color: var(--muted); font-size: 12px; font-weight: 500; letter-spacing: 0.5px; text-transform: uppercase; }}
    .summary-indicator {{
      margin-top: 14px;
      height: 4px;
      background: #30363d;
      border-radius: 2px;
      overflow: hidden;
    }}
    .summary-indicator-fill {{
      height: 100%;
      border-radius: 2px;
      transition: width 0.6s ease;
    }}
    .summary-card-glow {{
      position: absolute;
      top: 0; right: 0;
      width: 80px; height: 80px;
      border-radius: 50%;
      opacity: 0.08;
      transform: translate(30px, -30px);
    }}

    /* ── SECTIONS ── */
    .section {{
      margin-bottom: 48px;
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 16px;
      overflow: hidden;
    }}
    .section-header {{
      padding: 20px 28px;
      border-bottom: 1px solid var(--border);
      display: flex;
      align-items: center;
      gap: 14px;
      background: #0d1117;
    }}
    .section-number {{
      width: 32px; height: 32px;
      border-radius: 8px;
      background: #58a6ff18;
      border: 1px solid #58a6ff40;
      display: flex; align-items: center; justify-content: center;
      font-size: 14px; font-weight: 700; color: var(--accent);
    }}
    .section-title {{ font-size: 18px; font-weight: 700; flex: 1; }}
    .section-body {{ padding: 28px; }}

    /* ── BADGES ── */
    .badge {{
      display: inline-flex;
      align-items: center;
      gap: 4px;
      padding: 3px 10px;
      border-radius: 20px;
      font-size: 11px;
      font-weight: 600;
      letter-spacing: 0.5px;
    }}
    .table-badge {{
      display: inline-block;
      background: #58a6ff18;
      color: var(--accent);
      border: 1px solid #58a6ff30;
      padding: 2px 8px;
      border-radius: 4px;
      font-size: 11px;
      font-weight: 600;
      font-family: 'Courier New', monospace;
    }}

    /* ── TABLES ── */
    .data-table {{ width: 100%; border-collapse: collapse; }}
    .data-table th {{
      text-align: left;
      padding: 10px 14px;
      font-size: 11px;
      font-weight: 600;
      letter-spacing: 1px;
      text-transform: uppercase;
      color: var(--muted);
      background: #0d1117;
      border-bottom: 1px solid var(--border);
    }}
    .data-table td {{
      padding: 11px 14px;
      border-bottom: 1px solid #21262d;
      vertical-align: middle;
    }}
    .data-table tr:last-child td {{ border-bottom: none; }}
    .data-table tr:hover td {{ background: #ffffff06; }}
    .num {{ font-family: 'Courier New', monospace; text-align: right; color: #c9d1d9; }}
    .mono {{ font-family: 'Courier New', monospace; font-size: 12px; color: #c9d1d9; }}
    .muted {{ color: var(--muted) !important; }}
    .pct-cell {{ display: flex; align-items: center; gap: 10px; }}
    .mini-bar-bg {{ flex: 1; height: 4px; background: #30363d; border-radius: 2px; overflow: hidden; min-width: 60px; }}
    .mini-bar-fill {{ height: 100%; border-radius: 2px; }}

    /* ── CHECK CARDS (Consistency) ── */
    .check-cards-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
      gap: 16px;
      margin-bottom: 28px;
    }}
    .check-card {{
      background: #0d1117;
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 16px;
    }}
    .check-card-header {{
      display: flex;
      align-items: center;
      gap: 10px;
      margin-bottom: 8px;
    }}
    .check-icon {{ font-size: 18px; }}
    .check-label {{ flex: 1; font-weight: 600; font-size: 13px; }}
    .check-count {{ font-size: 22px; font-weight: 800; font-family: 'Courier New', monospace; }}
    .check-explanation {{ font-size: 12px; color: var(--muted); line-height: 1.5; margin-bottom: 10px; }}
    .check-status-row {{ display: flex; }}

    /* ── METRIC CARDS (Timeliness) ── */
    .metric-cards-grid {{
      display: grid;
      grid-template-columns: repeat(5, 1fr);
      gap: 14px;
      margin-bottom: 28px;
    }}
    .metric-card {{
      background: #0d1117;
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 16px;
      text-align: center;
    }}
    .metric-icon {{ font-size: 20px; margin-bottom: 8px; }}
    .metric-value {{ font-size: 26px; font-weight: 800; line-height: 1.1; margin-bottom: 4px; }}
    .metric-label {{ font-size: 12px; font-weight: 600; color: var(--muted); margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.5px; }}
    .metric-sub {{ font-size: 11px; color: #6e7681; margin-bottom: 10px; }}

    /* ── CHART CONTAINERS ── */
    .chart-wrap {{
      background: #0d1117;
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 20px;
      margin-top: 20px;
    }}
    .chart-title {{
      font-size: 13px;
      font-weight: 600;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.8px;
      margin-bottom: 16px;
    }}
    .chart-2col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-top: 20px; }}

    /* ── STAT CARDS ── */
    .stat-highlight {{
      background: #0d1117;
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 20px 24px;
      margin-bottom: 20px;
    }}
    .stat-highlight-row {{ display: flex; align-items: center; gap: 20px; flex-wrap: wrap; }}
    .stat-item {{ text-align: center; }}
    .stat-item-label {{ font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 4px; }}
    .stat-item-value {{ font-size: 24px; font-weight: 700; font-family: 'Courier New', monospace; }}
    .stat-divider {{ width: 1px; height: 48px; background: var(--border); }}

    /* ── GAP ALERT ── */
    .gap-alert {{
      background: #f8514918;
      border: 1px solid #f8514940;
      border-radius: 8px;
      padding: 12px 16px;
      color: var(--text);
      font-size: 13px;
      margin-top: 16px;
    }}
    .gap-ok {{
      background: #3fb95018;
      border: 1px solid #3fb95040;
      border-radius: 8px;
      padding: 12px 16px;
      font-size: 13px;
      margin-top: 16px;
    }}

    /* ── UNENRICHED BANNER ── */
    .unenriched-banner {{
      display: flex;
      align-items: center;
      gap: 16px;
      background: #d2992218;
      border: 1px solid #d2992240;
      border-radius: 10px;
      padding: 14px 20px;
      margin-top: 20px;
    }}
    .unenriched-icon {{ font-size: 24px; }}
    .unenriched-title {{ font-weight: 700; font-size: 14px; color: #d29922; }}
    .unenriched-sub {{ font-size: 12px; color: var(--muted); margin-top: 2px; }}
    .unenriched-count {{ font-size: 32px; font-weight: 800; color: #d29922; font-family: 'Courier New', monospace; margin-left: auto; }}

    /* ── TREND SECTION ── */
    .trend-section {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 16px;
      overflow: hidden;
      margin-bottom: 48px;
    }}
    .trend-header {{
      padding: 20px 28px;
      border-bottom: 1px solid var(--border);
      background: #0d1117;
      display: flex;
      align-items: center;
      gap: 12px;
    }}
    .trend-title {{ font-size: 18px; font-weight: 700; }}
    .trend-body {{ padding: 28px; }}

    /* ── FOOTER ── */
    .footer {{
      text-align: center;
      padding: 32px;
      border-top: 1px solid var(--border);
      color: var(--muted);
      font-size: 12px;
    }}
    .footer a {{ color: var(--accent); text-decoration: none; }}

    @media (max-width: 1100px) {{
      .summary-grid {{ grid-template-columns: repeat(2, 1fr); }}
      .metric-cards-grid {{ grid-template-columns: repeat(3, 1fr); }}
      .chart-2col {{ grid-template-columns: 1fr; }}
    }}
    @media (max-width: 700px) {{
      .main {{ padding: 16px; }}
      .page-header {{ padding: 20px 16px; }}
      .summary-grid {{ grid-template-columns: 1fr 1fr; }}
      .metric-cards-grid {{ grid-template-columns: 1fr 1fr; }}
    }}
  </style>
</head>
<body>

<!-- ═══════════════════════════════════════════════════
     HEADER
══════════════════════════════════════════════════════ -->
<header class="page-header">
  <div style="max-width:1400px;margin:0 auto">
    <nav class="header-nav">
      <a href="executive_dashboard.html">← Executive Dashboard</a>
      <span>/</span>
      <span>Data Quality Monitor</span>
    </nav>
    <div class="header-top">
      <div>
        <h1 class="header-title">📊 TransactGuard AML — Data Quality Monitor</h1>
        <div class="header-sub">
          <span class="dot"></span>
          Automated quality checks
          <span style="color:#30363d">|</span>
          Last run: {RUN_DATETIME}
          <span style="color:#30363d">|</span>
          <span style="color:#3fb950">50,000 transactions</span>
        </div>
      </div>
      <div class="health-score-box">
        <div class="health-score-label">Overall Health Score</div>
        <div class="health-score-number">{overall_score:.0f}<span class="health-score-denom">/100</span></div>
        <div class="health-score-sub">{warnings_count} warning{"s" if warnings_count != 1 else ""}, {critical_count} critical issue{"s" if critical_count != 1 else ""}</div>
      </div>
    </div>
  </div>
</header>

<main class="main">

  <!-- ── SUMMARY ROW ── -->
  <div class="summary-grid">
    <div class="summary-card">
      <div class="summary-card-glow" style="background:{score_color(comp['score'])}"></div>
      <div class="summary-card-icon">📋</div>
      <div class="summary-card-score" style="color:{score_color(comp['score'])}">{comp['score']:.0f}<span class="summary-card-denom">/100</span></div>
      <div class="summary-card-label">Completeness Score</div>
      <div class="summary-indicator"><div class="summary-indicator-fill" style="width:{comp['score']}%;background:{score_color(comp['score'])}"></div></div>
    </div>
    <div class="summary-card">
      <div class="summary-card-glow" style="background:{score_color(cons['score'])}"></div>
      <div class="summary-card-icon">🔗</div>
      <div class="summary-card-score" style="color:{score_color(cons['score'])}">{cons['score']:.0f}<span class="summary-card-denom">/100</span></div>
      <div class="summary-card-label">Consistency Score</div>
      <div class="summary-indicator"><div class="summary-indicator-fill" style="width:{cons['score']}%;background:{score_color(cons['score'])}"></div></div>
    </div>
    <div class="summary-card">
      <div class="summary-card-glow" style="background:{score_color(time_d['score'])}"></div>
      <div class="summary-card-icon">⏱</div>
      <div class="summary-card-score" style="color:{score_color(time_d['score'])}">{time_d['score']:.0f}<span class="summary-card-denom">/100</span></div>
      <div class="summary-card-label">Timeliness Score</div>
      <div class="summary-indicator"><div class="summary-indicator-fill" style="width:{time_d['score']}%;background:{score_color(time_d['score'])}"></div></div>
    </div>
    <div class="summary-card">
      <div class="summary-card-glow" style="background:{score_color(stat['score'])}"></div>
      <div class="summary-card-icon">📈</div>
      <div class="summary-card-score" style="color:{score_color(stat['score'])}">{stat['score']:.0f}<span class="summary-card-denom">/100</span></div>
      <div class="summary-card-label">Statistical Score</div>
      <div class="summary-indicator"><div class="summary-indicator-fill" style="width:{stat['score']}%;background:{score_color(stat['score'])}"></div></div>
    </div>
  </div>

  <!-- ══════════════════════════════════════════════════
       SECTION 1 — COMPLETENESS
  ═══════════════════════════════════════════════════ -->
  <div class="section">
    <div class="section-header">
      <div class="section-number">1</div>
      <div class="section-title">Completeness</div>
      {status_badge(comp_status, f"Score: {comp['score']:.0f}/100")}
    </div>
    <div class="section-body">

      <table class="data-table">
        <thead>
          <tr>
            <th>Table</th>
            <th>Field</th>
            <th style="text-align:right">NULL Count</th>
            <th>NULL %</th>
            <th>Status</th>
            <th style="text-align:right">Threshold</th>
          </tr>
        </thead>
        <tbody>
          {comp_table_rows}
        </tbody>
      </table>

      <div class="unenriched-banner">
        <div class="unenriched-icon">🔍</div>
        <div>
          <div class="unenriched-title">Missing Enrichment Detected</div>
          <div class="unenriched-sub">Transactions where counterparty_country IS NULL or counterparty_name is blank — unenriched data reduces SAR quality</div>
        </div>
        <div class="unenriched-count">{comp['unenriched_count']:,}</div>
        <div style="margin-left:8px">
          <div style="font-size:11px;color:var(--muted)">transactions</div>
          <div style="font-size:16px;font-weight:700;color:#d29922">{comp['unenriched_pct']}%</div>
        </div>
      </div>

      <div class="chart-wrap">
        <div class="chart-title">NULL Rate by Field — Horizontal Bar Chart</div>
        <canvas id="compChart" height="280"></canvas>
      </div>
    </div>
  </div>

  <!-- ══════════════════════════════════════════════════
       SECTION 2 — CONSISTENCY
  ═══════════════════════════════════════════════════ -->
  <div class="section">
    <div class="section-header">
      <div class="section-number">2</div>
      <div class="section-title">Consistency</div>
      {status_badge(cons_status, f"Score: {cons['score']:.0f}/100")}
    </div>
    <div class="section-body">
      <div class="check-cards-grid">
        {cons_cards_html}
      </div>

      <div class="chart-wrap">
        <div class="chart-title">Consistency Issue Counts — Horizontal Bar Chart</div>
        <canvas id="consChart" height="220"></canvas>
      </div>
    </div>
  </div>

  <!-- ══════════════════════════════════════════════════
       SECTION 3 — TIMELINESS
  ═══════════════════════════════════════════════════ -->
  <div class="section">
    <div class="section-header">
      <div class="section-number">3</div>
      <div class="section-title">Timeliness</div>
      {status_badge(time_status, f"Score: {time_d['score']:.0f}/100")}
    </div>
    <div class="section-body">
      <div class="metric-cards-grid">
        {time_cards}
      </div>

      {gap_html}

      <div class="chart-2col">
        <div class="chart-wrap">
          <div class="chart-title">Avg ETL Processing Lag by Week (Hours)</div>
          <canvas id="lagChart" height="220"></canvas>
        </div>
        <div class="chart-wrap">
          <div class="chart-title">Open Alert Age Buckets</div>
          <canvas id="bucketChart" height="220"></canvas>
        </div>
      </div>
    </div>
  </div>

  <!-- ══════════════════════════════════════════════════
       SECTION 4 — STATISTICAL ANOMALIES
  ═══════════════════════════════════════════════════ -->
  <div class="section">
    <div class="section-header">
      <div class="section-number">4</div>
      <div class="section-title">Statistical Anomalies</div>
      {status_badge(stat_status, f"Score: {stat['score']:.0f}/100")}
    </div>
    <div class="section-body">

      <!-- Anomaly Summary Table -->
      <div style="margin-bottom:28px">
        <div class="chart-title" style="margin-bottom:12px">Anomaly Summary</div>
        <table class="data-table">
          <thead>
            <tr>
              <th>Check</th>
              <th>Finding</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {stat_summary_rows}
          </tbody>
        </table>
      </div>

      <!-- Volume Anomaly Chart -->
      <div class="chart-wrap" style="margin-bottom:20px">
        <div class="chart-title">Weekly Transaction Volume — Anomaly Detection (Mean ± 2σ Band)</div>
        <canvas id="volChart" height="220"></canvas>
      </div>

      <!-- Amount Distribution Shift -->
      <div class="stat-highlight">
        <div style="font-size:12px;color:var(--muted);font-weight:600;letter-spacing:0.8px;text-transform:uppercase;margin-bottom:14px">Amount Distribution Shift — Last 30 Days vs Prior 11 Months</div>
        <div class="stat-highlight-row">
          <div class="stat-item">
            <div class="stat-item-label">Recent Avg (30d)</div>
            <div class="stat-item-value" style="color:#58a6ff">${stat['recent_avg_amount']:,.2f}</div>
          </div>
          <div class="stat-divider"></div>
          <div class="stat-item">
            <div class="stat-item-label">Historical Avg</div>
            <div class="stat-item-value" style="color:#8b949e">${stat['hist_avg_amount']:,.2f}</div>
          </div>
          <div class="stat-divider"></div>
          <div class="stat-item">
            <div class="stat-item-label">Deviation</div>
            <div class="stat-item-value" style="color:{status_color(stat['amount_status'])}">{stat['amount_deviation_pct']:+.1f}%</div>
          </div>
          <div class="stat-divider"></div>
          <div class="stat-item">
            <div class="stat-item-label">Status</div>
            <div style="margin-top:8px">{status_badge(stat['amount_status'])}</div>
          </div>
        </div>
      </div>

      <!-- New Countries Table -->
      <div style="margin-top:20px;margin-bottom:20px">
        <div class="chart-title" style="margin-bottom:12px">New Counterparty Countries (Last 30 Days vs Prior Period)</div>
        <table class="data-table">
          <thead>
            <tr>
              <th>Country Code</th>
              <th>First Seen</th>
              <th style="text-align:right">Tx Count</th>
              <th style="text-align:right">Total Amount</th>
              <th>Risk Level</th>
            </tr>
          </thead>
          <tbody>
            {new_countries_rows}
          </tbody>
        </table>
      </div>

      <div class="chart-2col">
        <!-- High-Value Chart -->
        <div class="chart-wrap">
          <div class="chart-title">High-Value Transactions (&gt;$100K) by Week</div>
          <div style="display:flex;gap:24px;margin-bottom:14px">
            <div class="stat-item">
              <div class="stat-item-label">Last 4wk Avg</div>
              <div style="font-size:20px;font-weight:700;color:#58a6ff;font-family:'Courier New',monospace">{stat['last4_hv_avg']:.1f}</div>
            </div>
            <div class="stat-item">
              <div class="stat-item-label">Historical Avg</div>
              <div style="font-size:20px;font-weight:700;color:var(--muted);font-family:'Courier New',monospace">{stat['hist_hv_avg']:.1f}</div>
            </div>
            <div class="stat-item">
              <div class="stat-item-label">Deviation</div>
              <div style="font-size:20px;font-weight:700;color:{status_color(stat['hv_status'])};font-family:'Courier New',monospace">{stat['hv_deviation_pct']:+.1f}%</div>
            </div>
          </div>
          <canvas id="hvChart" height="200"></canvas>
        </div>
        <!-- Alert Rate Chart -->
        <div class="chart-wrap">
          <div class="chart-title">Alert Rate by Week (% of Transactions Flagged)</div>
          <div style="margin-bottom:14px;font-size:12px;color:var(--muted)">
            Anomalous weeks (±2σ): <span style="color:{status_color('warning' if stat['alert_rate_anomalies'] else 'good')};font-weight:600">{len(stat['alert_rate_anomalies'])}</span>
          </div>
          <canvas id="rateChart" height="200"></canvas>
        </div>
      </div>

    </div>
  </div>

  <!-- ══════════════════════════════════════════════════
       QUALITY TREND
  ═══════════════════════════════════════════════════ -->
  <div class="trend-section">
    <div class="trend-header">
      <span style="font-size:20px">📆</span>
      <div class="trend-title">Data Quality Over Time — 12-Week Trend</div>
    </div>
    <div class="trend-body">
      <div class="chart-wrap" style="margin-bottom:28px">
        <div class="chart-title">Weekly Quality Scores</div>
        <canvas id="trendChart" height="200"></canvas>
      </div>

      <div class="chart-title" style="margin-bottom:12px">Recent Issues Log</div>
      <table class="data-table">
        <thead>
          <tr>
            <th>Date</th>
            <th>Check</th>
            <th>Issue</th>
            <th>Severity</th>
            <th>Resolved</th>
          </tr>
        </thead>
        <tbody>
          {issues_log_rows}
        </tbody>
      </table>
    </div>
  </div>

</main>

<footer class="footer">
  <div>TransactGuard AML Data Quality Monitor &mdash; Auto-generated on {RUN_DATETIME}</div>
  <div style="margin-top:4px">
    <a href="executive_dashboard.html">Executive Dashboard</a>
    &nbsp;·&nbsp;
    <a href="analyst_dashboard.html">Analyst Dashboard</a>
    &nbsp;·&nbsp;
    <a href="customer_risk_report.html">Customer Risk</a>
    &nbsp;·&nbsp;
    <a href="monthly_sar_summary.html">SAR Summary</a>
    &nbsp;·&nbsp;
    <a href="rule_effectiveness_report.html">Rule Effectiveness</a>
  </div>
</footer>

<!-- ═══════════════════════════════════════════════════
     CHARTS
══════════════════════════════════════════════════════ -->
<script>
const D = {json.dumps(chart_data, indent=2)};

const TOOLTIP_BG = '#1c2128';
const TOOLTIP_BORDER = '#30363d';
const GRID_COLOR = '#21262d';
const LABEL_COLOR = '#8b949e';
const fontFamily = "'Inter', sans-serif";

Chart.defaults.color = LABEL_COLOR;
Chart.defaults.font.family = fontFamily;
Chart.defaults.font.size = 12;

function baseOpts(title) {{
  return {{
    responsive: true,
    maintainAspectRatio: true,
    plugins: {{
      legend: {{ display: false }},
      tooltip: {{
        backgroundColor: TOOLTIP_BG,
        borderColor: TOOLTIP_BORDER,
        borderWidth: 1,
        titleColor: '#e6edf3',
        bodyColor: '#c9d1d9',
        padding: 10,
        cornerRadius: 6,
      }}
    }},
    scales: {{
      x: {{
        grid: {{ color: GRID_COLOR }},
        ticks: {{ color: LABEL_COLOR, maxRotation: 45 }}
      }},
      y: {{
        grid: {{ color: GRID_COLOR }},
        ticks: {{ color: LABEL_COLOR }}
      }}
    }}
  }};
}}

// ── Completeness NULL rates (horizontal bar)
new Chart(document.getElementById('compChart'), {{
  type: 'bar',
  data: {{
    labels: D.comp_labels,
    datasets: [{{
      label: 'NULL %',
      data: D.comp_values,
      backgroundColor: D.comp_colors.map(c => c + '99'),
      borderColor: D.comp_colors,
      borderWidth: 1.5,
      borderRadius: 4,
    }}]
  }},
  options: {{
    ...baseOpts('NULL Rate'),
    indexAxis: 'y',
    plugins: {{
      ...baseOpts().plugins,
      tooltip: {{
        ...baseOpts().plugins.tooltip,
        callbacks: {{
          label: ctx => ` ${{ctx.parsed.x.toFixed(2)}}%`
        }}
      }}
    }},
    scales: {{
      x: {{
        grid: {{ color: GRID_COLOR }},
        ticks: {{ color: LABEL_COLOR, callback: v => v + '%' }},
        title: {{ display: true, text: 'NULL Percentage', color: LABEL_COLOR }}
      }},
      y: {{ grid: {{ display: false }}, ticks: {{ color: '#c9d1d9', font: {{ size: 11 }} }} }}
    }}
  }}
}});

// ── Consistency issues (horizontal bar)
new Chart(document.getElementById('consChart'), {{
  type: 'bar',
  data: {{
    labels: D.cons_labels,
    datasets: [{{
      label: 'Count',
      data: D.cons_values,
      backgroundColor: D.cons_colors.map(c => c + '99'),
      borderColor: D.cons_colors,
      borderWidth: 1.5,
      borderRadius: 4,
    }}]
  }},
  options: {{
    ...baseOpts('Issues'),
    indexAxis: 'y',
    scales: {{
      x: {{
        grid: {{ color: GRID_COLOR }},
        ticks: {{ color: LABEL_COLOR }},
        title: {{ display: true, text: 'Count', color: LABEL_COLOR }}
      }},
      y: {{ grid: {{ display: false }}, ticks: {{ color: '#c9d1d9', font: {{ size: 11 }} }} }}
    }}
  }}
}});

// ── ETL Lag line chart
new Chart(document.getElementById('lagChart'), {{
  type: 'line',
  data: {{
    labels: D.weekly_lag_labels,
    datasets: [{{
      label: 'Avg ETL Lag (hrs)',
      data: D.weekly_lag_values,
      borderColor: '#58a6ff',
      backgroundColor: '#58a6ff18',
      fill: true,
      tension: 0.4,
      pointRadius: 2,
      pointHoverRadius: 5,
    }}]
  }},
  options: {{
    ...baseOpts('ETL Lag'),
    plugins: {{
      ...baseOpts().plugins,
      annotation: {{ annotations: {{}} }},
      tooltip: {{
        callbacks: {{ label: ctx => ` ${{ctx.parsed.y.toFixed(1)}} hrs` }}
      }}
    }},
    scales: {{
      x: {{ display: false }},
      y: {{
        grid: {{ color: GRID_COLOR }},
        ticks: {{ color: LABEL_COLOR, callback: v => v + 'h' }}
      }}
    }}
  }}
}});

// ── Alert age buckets (bar)
new Chart(document.getElementById('bucketChart'), {{
  type: 'bar',
  data: {{
    labels: D.bucket_labels,
    datasets: [{{
      label: 'Open Alerts',
      data: D.bucket_counts,
      backgroundColor: ['#3fb95099','#d2992299','#d2992299','#f8514999'],
      borderColor: ['#3fb950','#d29922','#d29922','#f85149'],
      borderWidth: 1.5,
      borderRadius: 4,
    }}]
  }},
  options: {{
    ...baseOpts('Buckets'),
    scales: {{
      x: {{ grid: {{ display: false }}, ticks: {{ color: '#c9d1d9' }} }},
      y: {{ grid: {{ color: GRID_COLOR }}, ticks: {{ color: LABEL_COLOR }} }}
    }}
  }}
}});

// ── Volume anomaly chart with band
const volCtx = document.getElementById('volChart').getContext('2d');
new Chart(volCtx, {{
  type: 'line',
  data: {{
    labels: D.vol_labels,
    datasets: [
      {{
        label: 'Mean + 2σ',
        data: D.vol_upper,
        borderColor: '#f8514940',
        borderWidth: 1,
        borderDash: [4, 4],
        pointRadius: 0,
        fill: '+1',
        backgroundColor: '#f8514910',
      }},
      {{
        label: 'Mean - 2σ',
        data: D.vol_lower,
        borderColor: '#f8514940',
        borderWidth: 1,
        borderDash: [4, 4],
        pointRadius: 0,
        fill: false,
      }},
      {{
        label: 'Mean',
        data: D.vol_mean,
        borderColor: '#58a6ff60',
        borderWidth: 1,
        borderDash: [6, 3],
        pointRadius: 0,
        fill: false,
      }},
      {{
        label: 'Weekly Volume',
        data: D.vol_values,
        borderColor: '#58a6ff',
        backgroundColor: '#58a6ff18',
        borderWidth: 2,
        pointRadius: ctx => {{
          const v = D.vol_values[ctx.dataIndex];
          return (v > D.vol_upper[0] || v < D.vol_lower[0]) ? 6 : 2;
        }},
        pointBackgroundColor: ctx => {{
          const v = D.vol_values[ctx.dataIndex];
          return (v > D.vol_upper[0] || v < D.vol_lower[0]) ? '#f85149' : '#58a6ff';
        }},
        tension: 0.4,
        fill: false,
      }}
    ]
  }},
  options: {{
    responsive: true,
    maintainAspectRatio: true,
    plugins: {{
      legend: {{
        display: true,
        labels: {{ color: LABEL_COLOR, usePointStyle: true, pointStyleWidth: 10 }}
      }},
      tooltip: {{
        backgroundColor: TOOLTIP_BG,
        borderColor: TOOLTIP_BORDER,
        borderWidth: 1,
        titleColor: '#e6edf3',
        bodyColor: '#c9d1d9',
        padding: 10,
        cornerRadius: 6,
      }}
    }},
    scales: {{
      x: {{ display: false }},
      y: {{
        grid: {{ color: GRID_COLOR }},
        ticks: {{ color: LABEL_COLOR }}
      }}
    }}
  }}
}});

// ── High-value chart
new Chart(document.getElementById('hvChart'), {{
  type: 'bar',
  data: {{
    labels: D.hv_labels,
    datasets: [{{
      label: '>$100K Transactions',
      data: D.hv_values,
      backgroundColor: '#d2992266',
      borderColor: '#d29922',
      borderWidth: 1.5,
      borderRadius: 3,
    }}]
  }},
  options: {{
    ...baseOpts('HV'),
    scales: {{
      x: {{ display: false }},
      y: {{ grid: {{ color: GRID_COLOR }}, ticks: {{ color: LABEL_COLOR }} }}
    }}
  }}
}});

// ── Alert rate chart
new Chart(document.getElementById('rateChart'), {{
  type: 'line',
  data: {{
    labels: D.rate_labels,
    datasets: [{{
      label: 'Alert Rate %',
      data: D.rate_values,
      borderColor: '#d29922',
      backgroundColor: '#d2992218',
      fill: true,
      tension: 0.4,
      pointRadius: 2,
      pointHoverRadius: 5,
    }}]
  }},
  options: {{
    ...baseOpts('Rate'),
    scales: {{
      x: {{ display: false }},
      y: {{
        grid: {{ color: GRID_COLOR }},
        ticks: {{ color: LABEL_COLOR, callback: v => v.toFixed(1) + '%' }}
      }}
    }},
    plugins: {{
      ...baseOpts().plugins,
      tooltip: {{
        callbacks: {{ label: ctx => ` ${{ctx.parsed.y.toFixed(2)}}%` }}
      }}
    }}
  }}
}});

// ── Quality Trend chart
new Chart(document.getElementById('trendChart'), {{
  type: 'line',
  data: {{
    labels: D.trend_labels,
    datasets: [
      {{
        label: 'Overall Score',
        data: D.trend_overall,
        borderColor: '#58a6ff',
        backgroundColor: '#58a6ff18',
        borderWidth: 2.5,
        tension: 0.4,
        fill: false,
        pointRadius: 3,
      }},
      {{
        label: 'Completeness',
        data: D.trend_comp,
        borderColor: '#3fb950',
        borderWidth: 1.5,
        tension: 0.4,
        fill: false,
        pointRadius: 2,
        borderDash: [4, 3],
      }},
      {{
        label: 'Consistency',
        data: D.trend_cons,
        borderColor: '#d29922',
        borderWidth: 1.5,
        tension: 0.4,
        fill: false,
        pointRadius: 2,
        borderDash: [4, 3],
      }}
    ]
  }},
  options: {{
    responsive: true,
    maintainAspectRatio: true,
    plugins: {{
      legend: {{
        display: true,
        labels: {{ color: LABEL_COLOR, usePointStyle: true }}
      }},
      tooltip: {{
        backgroundColor: TOOLTIP_BG,
        borderColor: TOOLTIP_BORDER,
        borderWidth: 1,
        titleColor: '#e6edf3',
        bodyColor: '#c9d1d9',
        padding: 10,
        cornerRadius: 6,
        callbacks: {{ label: ctx => ` ${{ctx.dataset.label}}: ${{ctx.parsed.y.toFixed(1)}}` }}
      }}
    }},
    scales: {{
      x: {{ grid: {{ color: GRID_COLOR }}, ticks: {{ color: LABEL_COLOR }} }},
      y: {{
        grid: {{ color: GRID_COLOR }},
        ticks: {{ color: LABEL_COLOR, callback: v => v + '/100' }},
        min: 50,
        max: 100
      }}
    }}
  }}
}});
</script>
</body>
</html>"""

    return html


def main():
    print("Connecting to database...")
    conn = get_conn()

    print("Running completeness checks...")
    comp = collect_completeness(conn)

    print("Running consistency checks...")
    cons = collect_consistency(conn)

    print("Running timeliness checks...")
    time_d = collect_timeliness(conn)

    print("Running statistical anomaly checks...")
    stat = collect_statistical(conn)

    conn.close()

    overall_score = compute_overall(comp["score"], cons["score"], time_d["score"], stat["score"])
    trend = build_trend_data(comp, cons, time_d, stat)

    print(f"\nScores:")
    print(f"  Completeness : {comp['score']:.1f}/100")
    print(f"  Consistency  : {cons['score']:.1f}/100")
    print(f"  Timeliness   : {time_d['score']:.1f}/100")
    print(f"  Statistical  : {stat['score']:.1f}/100")
    print(f"  OVERALL      : {overall_score:.1f}/100")

    print("\nGenerating HTML...")
    html = generate_html(comp, cons, time_d, stat, trend, overall_score)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        f.write(html)

    size_kb = os.path.getsize(OUTPUT_PATH) / 1024
    print(f"\nOutput written to: {OUTPUT_PATH}")
    print(f"File size: {size_kb:.1f} KB")

    if size_kb < 50:
        print("WARNING: File is smaller than 50KB — check output!")
    else:
        print("OK: File size check passed (> 50KB)")


if __name__ == '__main__':
    main()
