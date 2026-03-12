"""
Generate Executive Intelligence Dashboard for TransactGuard AML
Reads from data/transactions.db and outputs output/executive_dashboard.html
"""

import sqlite3
import json
import os
from datetime import datetime, timedelta
from collections import defaultdict

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "transactions.db")
OUTPUT_PATH = os.path.join(BASE_DIR, "output", "executive_dashboard.html")

# ---------------------------------------------------------------------------
# Data extraction
# ---------------------------------------------------------------------------

def fetch_all_data(conn):
    cur = conn.cursor()
    data = {}

    # --- KPI 1: Total Transactions ---
    cur.execute("SELECT COUNT(*) FROM transactions")
    data["total_transactions"] = cur.fetchone()[0]

    # Transactions per week (last 52 weeks, for sparkline - use last 12 weeks)
    cur.execute("""
        SELECT strftime('%Y-%W', transaction_date) AS wk, COUNT(*) AS cnt
        FROM transactions
        WHERE transaction_date >= date('now', '-84 days')
        GROUP BY wk ORDER BY wk
    """)
    weekly_txn = cur.fetchall()
    data["sparkline_txn"] = [r[1] for r in weekly_txn]

    # --- KPI 2: Active Alerts ---
    cur.execute("""
        SELECT status, COUNT(*) FROM alerts
        GROUP BY status
    """)
    status_counts = dict(cur.fetchall())
    data["status_counts"] = status_counts
    data["active_alerts"] = (
        status_counts.get("new", 0) +
        status_counts.get("under_review", 0) +
        status_counts.get("escalated", 0)
    )

    # Severity breakdown for active alerts
    cur.execute("""
        SELECT severity, COUNT(*) FROM alerts
        WHERE status IN ('new','under_review','escalated')
        GROUP BY severity
    """)
    data["active_severity"] = dict(cur.fetchall())

    # --- KPI 3: Alert Closure Rate ---
    cur.execute("SELECT COUNT(*) FROM alerts")
    data["total_alerts"] = cur.fetchone()[0]
    closed = status_counts.get("true_positive", 0) + status_counts.get("false_positive", 0)
    data["closed_alerts"] = closed
    data["closure_rate"] = round(closed / data["total_alerts"] * 100, 1) if data["total_alerts"] > 0 else 0

    # --- KPI 4: Avg Resolution Time ---
    cur.execute("""
        SELECT AVG(
            (julianday(updated_at) - julianday(created_at))
        ) FROM alerts
        WHERE status IN ('true_positive','false_positive')
          AND updated_at IS NOT NULL AND created_at IS NOT NULL
    """)
    avg_days = cur.fetchone()[0]
    data["avg_resolution_days"] = round(avg_days, 1) if avg_days else 0.0

    # --- KPI 5: Total Flagged Amount ---
    cur.execute("SELECT SUM(flagged_amount) FROM alerts")
    total_flagged = cur.fetchone()[0] or 0
    data["total_flagged_amount"] = total_flagged
    data["total_flagged_display"] = f"${total_flagged/1_000_000:.1f}M"

    # --- KPI 6: High-Risk Customers ---
    cur.execute("""
        SELECT COUNT(*) FROM customers
        WHERE risk_rating IN ('high','critical')
    """)
    data["high_risk_customers"] = cur.fetchone()[0]

    # --- Chart 1: Transaction Volume & Alert Frequency by week (52 weeks) ---
    cur.execute("""
        SELECT strftime('%Y-%W', transaction_date) AS wk, COUNT(*) AS cnt
        FROM transactions
        WHERE transaction_date >= date('now', '-364 days')
        GROUP BY wk ORDER BY wk
    """)
    txn_weekly = cur.fetchall()

    cur.execute("""
        SELECT strftime('%Y-%W', created_at) AS wk, COUNT(*) AS cnt
        FROM alerts
        WHERE created_at >= date('now', '-364 days')
        GROUP BY wk ORDER BY wk
    """)
    alert_weekly = cur.fetchall()

    # Merge on week labels
    txn_dict = dict(txn_weekly)
    alert_dict = dict(alert_weekly)
    all_weeks = sorted(set(list(txn_dict.keys()) + list(alert_dict.keys())))

    data["chart_weekly_labels"] = all_weeks
    data["chart_weekly_txn"] = [txn_dict.get(w, 0) for w in all_weeks]
    data["chart_weekly_alerts"] = [alert_dict.get(w, 0) for w in all_weeks]

    # --- Chart 2: Alert Severity Distribution ---
    cur.execute("""
        SELECT severity, COUNT(*) FROM alerts GROUP BY severity ORDER BY severity
    """)
    severity_dist = dict(cur.fetchall())
    data["severity_dist"] = {
        "critical": severity_dist.get("critical", 0),
        "high":     severity_dist.get("high", 0),
        "medium":   severity_dist.get("medium", 0),
        "low":      severity_dist.get("low", 0),
    }

    # --- Chart 3: Top 10 Rules by Alert Count ---
    cur.execute("""
        SELECT r.rule_name, COUNT(*) AS cnt
        FROM alerts a
        JOIN rules r ON a.rule_id = r.rule_id
        GROUP BY r.rule_name
        ORDER BY cnt DESC
        LIMIT 10
    """)
    top_rules = cur.fetchall()
    data["top_rules_labels"] = [r[0] for r in top_rules]
    data["top_rules_counts"] = [r[1] for r in top_rules]

    # --- Chart 4: Geographic Risk Distribution (top 10 countries) ---
    cur.execute("""
        SELECT t.counterparty_country, cr.risk_level, COUNT(*) AS cnt
        FROM alerts a
        JOIN transactions t ON a.transaction_id = t.transaction_id
        LEFT JOIN country_risk cr ON t.counterparty_country = cr.country_code
        GROUP BY t.counterparty_country
        ORDER BY cnt DESC
        LIMIT 10
    """)
    geo_data = cur.fetchall()
    data["geo_labels"]     = [r[0] for r in geo_data]
    data["geo_counts"]     = [r[2] for r in geo_data]
    data["geo_risk_levels"] = [r[1] or "unknown" for r in geo_data]

    # --- Chart 5: Alert Status Funnel ---
    funnel_order = ["new", "under_review", "escalated", "true_positive", "false_positive"]
    data["funnel_labels"] = ["New", "Under Review", "Escalated", "True Positive", "False Positive"]
    data["funnel_counts"] = [status_counts.get(s, 0) for s in funnel_order]

    # --- Chart 6: Rule Precision vs Recall scatter ---
    cur.execute("""
        SELECT rule_name, precision_score, recall_score, total_alerts_generated
        FROM rules WHERE enabled = 1
        ORDER BY rule_name
    """)
    rules_data = cur.fetchall()
    data["rule_scatter"] = [
        {
            "name": r[0],
            "abbr": "".join(w[0] for w in r[0].split())[:4].upper(),
            "precision": round(r[1], 4),
            "recall": round(r[2], 4),
            "alerts": r[3],
        }
        for r in rules_data
    ]

    # --- Recent Alerts Table (last 20) ---
    cur.execute("""
        SELECT a.alert_id, a.created_at, c.name, a.flagged_amount,
               r.rule_name, a.severity, a.status, a.assigned_to
        FROM alerts a
        JOIN customers c ON a.customer_id = c.customer_id
        JOIN rules r ON a.rule_id = r.rule_id
        ORDER BY a.created_at DESC
        LIMIT 20
    """)
    recent = cur.fetchall()
    data["recent_alerts"] = [
        {
            "alert_id":     r[0],
            "date":         r[1][:10] if r[1] else "",
            "customer":     r[2],
            "amount":       f"${r[3]:,.2f}" if r[3] else "$0.00",
            "rule":         r[4],
            "severity":     r[5],
            "status":       r[6],
            "assigned_to":  r[7] or "Unassigned",
        }
        for r in recent
    ]

    # --- Rules Performance Table (all rules) ---
    cur.execute("""
        SELECT rule_id, rule_name, rule_category, precision_score, recall_score,
               total_alerts_generated, true_positives, false_positives, threshold_value
        FROM rules WHERE enabled = 1
        ORDER BY total_alerts_generated DESC
    """)
    all_rules = cur.fetchall()
    data["rules_table"] = [
        {
            "rule_id":      r[0],
            "rule_name":    r[1],
            "category":     r[2],
            "precision":    round(r[3], 3),
            "recall":       round(r[4], 3),
            "total_alerts": r[5],
            "true_pos":     r[6],
            "false_pos":    r[7],
            "threshold":    r[8],
            "f1":           round(
                                2 * r[3] * r[4] / (r[3] + r[4])
                                if (r[3] + r[4]) > 0 else 0, 3
                            ),
        }
        for r in all_rules
    ]

    # --- Monthly transaction amounts (last 12 months) ---
    cur.execute("""
        SELECT strftime('%Y-%m', transaction_date) AS mo,
               COUNT(*) AS cnt,
               SUM(amount) AS total_amt
        FROM transactions
        WHERE transaction_date >= date('now', '-365 days')
        GROUP BY mo ORDER BY mo
    """)
    monthly = cur.fetchall()
    data["monthly_labels"]  = [r[0] for r in monthly]
    data["monthly_counts"]  = [r[1] for r in monthly]
    data["monthly_amounts"] = [round(r[2] or 0, 2) for r in monthly]

    # --- Customer risk breakdown ---
    cur.execute("""
        SELECT risk_rating, COUNT(*) FROM customers GROUP BY risk_rating
    """)
    data["customer_risk"] = dict(cur.fetchall())

    # --- Alert assignment workload ---
    cur.execute("""
        SELECT COALESCE(assigned_to, 'Unassigned') AS analyst,
               COUNT(*) AS cnt,
               SUM(CASE WHEN status='true_positive' THEN 1 ELSE 0 END) AS tp,
               SUM(CASE WHEN status='false_positive' THEN 1 ELSE 0 END) AS fp
        FROM alerts
        GROUP BY analyst
        ORDER BY cnt DESC
        LIMIT 8
    """)
    workload = cur.fetchall()
    data["workload"] = [
        {"analyst": r[0], "total": r[1], "tp": r[2], "fp": r[3]}
        for r in workload
    ]

    return data


# ---------------------------------------------------------------------------
# HTML generation
# ---------------------------------------------------------------------------

def format_number(n):
    """Format integer with commas."""
    return f"{n:,}"


def build_html(data):
    weekly_labels_js = json.dumps(data["chart_weekly_labels"])
    weekly_txn_js    = json.dumps(data["chart_weekly_txn"])
    weekly_alerts_js = json.dumps(data["chart_weekly_alerts"])
    severity_js      = json.dumps(data["severity_dist"])
    top_rules_labels_js = json.dumps(data["top_rules_labels"])
    top_rules_counts_js = json.dumps(data["top_rules_counts"])
    geo_labels_js    = json.dumps(data["geo_labels"])
    geo_counts_js    = json.dumps(data["geo_counts"])
    geo_risks_js     = json.dumps(data["geo_risk_levels"])
    funnel_labels_js = json.dumps(data["funnel_labels"])
    funnel_counts_js = json.dumps(data["funnel_counts"])
    scatter_js       = json.dumps(data["rule_scatter"])
    recent_js        = json.dumps(data["recent_alerts"])
    sparkline_js     = json.dumps(data["sparkline_txn"])
    active_sev_js    = json.dumps(data["active_severity"])
    rules_table_js   = json.dumps(data["rules_table"])
    monthly_labels_js  = json.dumps(data["monthly_labels"])
    monthly_counts_js  = json.dumps(data["monthly_counts"])
    monthly_amounts_js = json.dumps(data["monthly_amounts"])
    workload_js        = json.dumps(data["workload"])
    customer_risk_js   = json.dumps(data["customer_risk"])

    total_txn_fmt   = format_number(data["total_transactions"])
    active_fmt      = format_number(data["active_alerts"])
    hrc_fmt         = format_number(data["high_risk_customers"])
    closure_fmt     = f"{data['closure_rate']}%"
    avg_res_fmt     = f"{data['avg_resolution_days']}d"
    flagged_fmt     = data["total_flagged_display"]

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>TransactGuard AML — Executive Intelligence Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
/* ===== RESET & BASE ===== */
*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

:root {{
  --bg:           #0d1117;
  --card:         #161b22;
  --card-hover:   #1c2128;
  --border:       #30363d;
  --border-subtle:#21262d;
  --text-primary: #e6edf3;
  --text-secondary:#8b949e;
  --text-muted:   #6e7681;
  --blue:         #58a6ff;
  --green:        #3fb950;
  --red:          #f78166;
  --orange:       #f0883e;
  --yellow:       #d29922;
  --purple:       #bc8cff;
  --critical:     #f85149;
  --high:         #f0883e;
  --medium:       #d29922;
  --low:          #3fb950;
}}

html, body {{
  background: var(--bg);
  color: var(--text-primary);
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
  font-size: 14px;
  line-height: 1.5;
  min-height: 100vh;
}}

/* ===== SCROLLBAR ===== */
::-webkit-scrollbar {{ width: 6px; height: 6px; }}
::-webkit-scrollbar-track {{ background: var(--bg); }}
::-webkit-scrollbar-thumb {{ background: var(--border); border-radius: 3px; }}

/* ===== HEADER ===== */
.header {{
  background: var(--card);
  border-bottom: 1px solid var(--border);
  padding: 0 32px;
  height: 64px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  position: sticky;
  top: 0;
  z-index: 100;
}}

.header-logo {{
  display: flex;
  align-items: center;
  gap: 10px;
  font-size: 18px;
  font-weight: 700;
  color: var(--text-primary);
  letter-spacing: -0.3px;
  white-space: nowrap;
}}

.header-logo .logo-icon {{
  font-size: 22px;
}}

.header-logo .logo-accent {{
  color: var(--blue);
}}

.header-center {{
  text-align: center;
  flex: 1;
  padding: 0 24px;
}}

.header-title {{
  font-size: 16px;
  font-weight: 600;
  color: var(--text-primary);
  letter-spacing: 0.5px;
}}

.header-subtitle {{
  font-size: 11px;
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 1px;
}}

.header-meta {{
  text-align: right;
  white-space: nowrap;
}}

.header-meta .date-range {{
  font-size: 12px;
  color: var(--text-secondary);
  font-weight: 500;
}}

.header-meta .generated {{
  font-size: 11px;
  color: var(--text-muted);
  margin-top: 2px;
}}

/* Gradient accent bar */
.header-bar {{
  height: 3px;
  background: linear-gradient(90deg, #58a6ff 0%, #3fb950 30%, #d29922 60%, #f78166 100%);
}}

/* ===== MAIN CONTENT ===== */
.main {{
  padding: 28px 32px 48px;
  max-width: 1600px;
  margin: 0 auto;
}}

/* ===== SECTION TITLE ===== */
.section-title {{
  font-size: 11px;
  font-weight: 600;
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 1.2px;
  margin-bottom: 14px;
  display: flex;
  align-items: center;
  gap: 8px;
}}

.section-title::after {{
  content: '';
  flex: 1;
  height: 1px;
  background: var(--border-subtle);
}}

/* ===== KPI CARDS ===== */
.kpi-grid {{
  display: grid;
  grid-template-columns: repeat(6, 1fr);
  gap: 16px;
  margin-bottom: 28px;
}}

@media (max-width: 1400px) {{
  .kpi-grid {{ grid-template-columns: repeat(3, 1fr); }}
}}

.kpi-card {{
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 20px;
  position: relative;
  overflow: hidden;
  transition: border-color 0.2s, transform 0.2s;
  cursor: default;
}}

.kpi-card::before {{
  content: '';
  position: absolute;
  top: 0; left: 0; right: 0;
  height: 3px;
  border-radius: 10px 10px 0 0;
}}

.kpi-card.blue::before  {{ background: var(--blue); }}
.kpi-card.green::before {{ background: var(--green); }}
.kpi-card.red::before   {{ background: var(--red); }}
.kpi-card.yellow::before {{ background: var(--yellow); }}
.kpi-card.orange::before {{ background: var(--orange); }}
.kpi-card.purple::before {{ background: var(--purple); }}

.kpi-card:hover {{
  border-color: var(--blue);
  transform: translateY(-2px);
}}

.kpi-top {{
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  margin-bottom: 12px;
}}

.kpi-icon {{
  font-size: 22px;
  line-height: 1;
}}

.kpi-label {{
  font-size: 11px;
  font-weight: 600;
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 0.8px;
  margin-bottom: 6px;
}}

.kpi-value {{
  font-size: 32px;
  font-weight: 800;
  color: var(--text-primary);
  letter-spacing: -1px;
  line-height: 1;
  margin-bottom: 8px;
}}

.kpi-sub {{
  font-size: 11px;
  color: var(--text-secondary);
  display: flex;
  align-items: center;
  gap: 4px;
}}

.kpi-trend-up   {{ color: var(--green); }}
.kpi-trend-down {{ color: var(--red); }}

/* Mini sparkline canvas */
.kpi-sparkline {{
  width: 100%;
  height: 36px;
  margin-top: 10px;
}}

/* Mini severity bar */
.sev-bar {{
  margin-top: 10px;
  height: 6px;
  border-radius: 3px;
  overflow: hidden;
  display: flex;
  gap: 2px;
}}

.sev-seg {{
  height: 100%;
  border-radius: 2px;
  transition: opacity 0.2s;
}}

.sev-legend {{
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-top: 6px;
}}

.sev-legend-item {{
  display: flex;
  align-items: center;
  gap: 4px;
  font-size: 10px;
  color: var(--text-secondary);
}}

.sev-dot {{
  width: 6px; height: 6px;
  border-radius: 50%;
}}

/* ===== CHART CARDS ===== */
.chart-row {{
  display: grid;
  gap: 16px;
  margin-bottom: 20px;
}}

.chart-row-60-40 {{ grid-template-columns: 60fr 40fr; }}
.chart-row-50-50 {{ grid-template-columns: 1fr 1fr; }}

@media (max-width: 1100px) {{
  .chart-row-60-40,
  .chart-row-50-50 {{ grid-template-columns: 1fr; }}
}}

.chart-card {{
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 22px;
  display: flex;
  flex-direction: column;
}}

.chart-card-title {{
  font-size: 13px;
  font-weight: 600;
  color: var(--text-primary);
  margin-bottom: 4px;
  display: flex;
  align-items: center;
  gap: 8px;
}}

.chart-card-subtitle {{
  font-size: 11px;
  color: var(--text-muted);
  margin-bottom: 18px;
}}

.chart-wrapper {{
  position: relative;
  flex: 1;
  min-height: 0;
}}

/* ===== FUNNEL CHART ===== */
.funnel-container {{
  display: flex;
  flex-direction: column;
  gap: 10px;
  padding: 8px 0;
}}

.funnel-item {{
  display: flex;
  align-items: center;
  gap: 12px;
}}

.funnel-label {{
  font-size: 12px;
  color: var(--text-secondary);
  font-weight: 500;
  width: 110px;
  flex-shrink: 0;
}}

.funnel-bar-wrap {{
  flex: 1;
  background: var(--border-subtle);
  border-radius: 4px;
  height: 28px;
  overflow: hidden;
}}

.funnel-bar {{
  height: 100%;
  border-radius: 4px;
  display: flex;
  align-items: center;
  padding-left: 10px;
  font-size: 12px;
  font-weight: 600;
  color: #fff;
  transition: width 0.8s cubic-bezier(0.4,0,0.2,1);
  min-width: 40px;
  white-space: nowrap;
}}

.funnel-pct {{
  font-size: 11px;
  color: var(--text-muted);
  width: 38px;
  text-align: right;
  flex-shrink: 0;
}}

/* ===== RECENT ALERTS TABLE ===== */
.table-card {{
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 10px;
  overflow: hidden;
  margin-top: 4px;
}}

.table-header {{
  padding: 18px 22px;
  border-bottom: 1px solid var(--border);
  display: flex;
  align-items: center;
  justify-content: space-between;
}}

.table-title {{
  font-size: 14px;
  font-weight: 600;
  color: var(--text-primary);
  display: flex;
  align-items: center;
  gap: 8px;
}}

.table-hint {{
  font-size: 11px;
  color: var(--text-muted);
  display: flex;
  align-items: center;
  gap: 4px;
}}

.table-scroll {{
  overflow-x: auto;
}}

table {{
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
}}

thead th {{
  background: #1c2128;
  color: var(--text-secondary);
  font-weight: 600;
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.6px;
  padding: 12px 16px;
  text-align: left;
  border-bottom: 1px solid var(--border);
  cursor: pointer;
  user-select: none;
  white-space: nowrap;
  transition: color 0.15s;
}}

thead th:hover {{ color: var(--blue); }}

thead th .sort-icon {{
  display: inline-block;
  margin-left: 4px;
  opacity: 0.4;
  font-size: 10px;
}}

thead th.sorted .sort-icon {{ opacity: 1; color: var(--blue); }}

tbody tr:nth-child(even) {{ background: var(--card-hover); }}
tbody tr:nth-child(odd)  {{ background: var(--card); }}

tbody tr {{
  border-bottom: 1px solid var(--border-subtle);
  transition: background 0.15s;
}}

tbody tr:hover {{
  background: #22303f !important;
}}

tbody td {{
  padding: 11px 16px;
  color: var(--text-primary);
  vertical-align: middle;
}}

.td-id {{ font-family: 'Courier New', monospace; font-size: 12px; color: var(--text-secondary); }}
.td-date {{ color: var(--text-secondary); white-space: nowrap; }}
.td-customer {{ font-weight: 500; }}
.td-amount {{ font-weight: 600; color: var(--text-primary); text-align: right; font-family: 'Courier New', monospace; }}
.td-rule {{ color: var(--text-secondary); max-width: 180px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
.td-assigned {{ color: var(--text-muted); font-size: 12px; }}

/* ===== BADGES ===== */
.badge {{
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 3px 9px;
  border-radius: 20px;
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.3px;
  white-space: nowrap;
}}

.badge::before {{
  content: '';
  width: 5px; height: 5px;
  border-radius: 50%;
  background: currentColor;
  opacity: 0.8;
}}

/* Severity badges */
.badge-critical {{ background: rgba(248,81,73,0.15);  color: #f85149; border: 1px solid rgba(248,81,73,0.3); }}
.badge-high     {{ background: rgba(240,136,62,0.15); color: #f0883e; border: 1px solid rgba(240,136,62,0.3); }}
.badge-medium   {{ background: rgba(210,153,34,0.15); color: #d29922; border: 1px solid rgba(210,153,34,0.3); }}
.badge-low      {{ background: rgba(63,185,80,0.15);  color: #3fb950; border: 1px solid rgba(63,185,80,0.3); }}

/* Status badges */
.badge-new            {{ background: rgba(88,166,255,0.12); color: #58a6ff; border: 1px solid rgba(88,166,255,0.25); }}
.badge-under_review   {{ background: rgba(210,153,34,0.12); color: #d29922; border: 1px solid rgba(210,153,34,0.25); }}
.badge-escalated      {{ background: rgba(248,81,73,0.12);  color: #f85149; border: 1px solid rgba(248,81,73,0.25); }}
.badge-true_positive  {{ background: rgba(63,185,80,0.12);  color: #3fb950; border: 1px solid rgba(63,185,80,0.25); }}
.badge-false_positive {{ background: rgba(110,118,129,0.12);color: #8b949e; border: 1px solid rgba(110,118,129,0.25); }}

/* ===== RULES TABLE ===== */
.rules-table-card {{
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 10px;
  overflow: hidden;
  margin-bottom: 20px;
}}

.rules-table-card table {{ font-size: 12px; }}
.rules-table-card thead th {{ font-size: 10px; padding: 10px 14px; }}
.rules-table-card tbody td {{ padding: 9px 14px; }}

.perf-bar-wrap {{
  width: 80px;
  height: 6px;
  background: var(--border-subtle);
  border-radius: 3px;
  overflow: hidden;
  display: inline-block;
  vertical-align: middle;
}}

.perf-bar {{
  height: 100%;
  border-radius: 3px;
}}

.f1-good   {{ color: var(--green); font-weight: 600; }}
.f1-ok     {{ color: var(--yellow); font-weight: 600; }}
.f1-poor   {{ color: var(--red); font-weight: 600; }}

.cat-badge {{
  display: inline-block;
  padding: 2px 7px;
  border-radius: 4px;
  font-size: 10px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.4px;
}}

.cat-structuring {{ background: rgba(88,166,255,0.12); color: #58a6ff; }}
.cat-velocity    {{ background: rgba(240,136,62,0.12);  color: #f0883e; }}
.cat-geographic  {{ background: rgba(63,185,80,0.12);   color: #3fb950; }}
.cat-behavior    {{ background: rgba(210,153,34,0.12);  color: #d29922; }}
.cat-network     {{ background: rgba(188,140,255,0.12); color: #bc8cff; }}

/* ===== WORKLOAD SECTION ===== */
.two-col {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
  margin-bottom: 20px;
}}

@media (max-width: 900px) {{ .two-col {{ grid-template-columns: 1fr; }} }}

/* ===== FOOTER ===== */
.footer {{
  text-align: center;
  padding: 32px;
  color: var(--text-muted);
  font-size: 11px;
  border-top: 1px solid var(--border-subtle);
  margin-top: 40px;
  letter-spacing: 0.5px;
}}

.footer span {{ color: var(--blue); font-weight: 500; }}
</style>
</head>
<body>

<!-- ===== HEADER ===== -->
<header>
  <div class="header">
    <div class="header-logo">
      <span class="logo-icon">🔍</span>
      <span>Transact<span class="logo-accent">Guard</span> AML</span>
    </div>
    <div class="header-center">
      <div class="header-title">Executive Intelligence Dashboard</div>
      <div class="header-subtitle">Anti-Money Laundering · Compliance Analytics</div>
    </div>
    <div class="header-meta">
      <div class="date-range">Data range: Mar 2025 – Mar 2026</div>
      <div class="generated">Generated: Mar 12, 2026</div>
    </div>
  </div>
  <div class="header-bar"></div>
</header>

<!-- ===== MAIN ===== -->
<main class="main">

  <!-- KPI CARDS -->
  <div class="section-title">Key Performance Indicators</div>
  <div class="kpi-grid">

    <!-- KPI 1: Total Transactions -->
    <div class="kpi-card blue">
      <div class="kpi-top">
        <div>
          <div class="kpi-label">Total Transactions</div>
          <div class="kpi-value">{total_txn_fmt}</div>
        </div>
        <div class="kpi-icon">💳</div>
      </div>
      <canvas class="kpi-sparkline" id="sparkTxn"></canvas>
      <div class="kpi-sub">Last 12 weeks trend</div>
    </div>

    <!-- KPI 2: Active Alerts -->
    <div class="kpi-card red">
      <div class="kpi-top">
        <div>
          <div class="kpi-label">Active Alerts</div>
          <div class="kpi-value">{active_fmt}</div>
        </div>
        <div class="kpi-icon">🚨</div>
      </div>
      <div class="sev-bar" id="sevBar"></div>
      <div class="sev-legend" id="sevLegend"></div>
    </div>

    <!-- KPI 3: Alert Closure Rate -->
    <div class="kpi-card green">
      <div class="kpi-top">
        <div>
          <div class="kpi-label">Alert Closure Rate</div>
          <div class="kpi-value">{closure_fmt}</div>
        </div>
        <div class="kpi-icon">✅</div>
      </div>
      <div class="kpi-sub">
        <span class="kpi-trend-up">↑</span>
        {format_number(data['closed_alerts'])} of {format_number(data['total_alerts'])} alerts closed
      </div>
    </div>

    <!-- KPI 4: Avg Resolution Time -->
    <div class="kpi-card yellow">
      <div class="kpi-top">
        <div>
          <div class="kpi-label">Avg Resolution Time</div>
          <div class="kpi-value">{avg_res_fmt}</div>
        </div>
        <div class="kpi-icon">⏱️</div>
      </div>
      <div class="kpi-sub">Days from creation to closure</div>
    </div>

    <!-- KPI 5: Total Flagged Amount -->
    <div class="kpi-card orange">
      <div class="kpi-top">
        <div>
          <div class="kpi-label">Total Flagged Amount</div>
          <div class="kpi-value">{flagged_fmt}</div>
        </div>
        <div class="kpi-icon">💰</div>
      </div>
      <div class="kpi-sub">Across all {format_number(data['total_alerts'])} alerts</div>
    </div>

    <!-- KPI 6: High-Risk Customers -->
    <div class="kpi-card purple">
      <div class="kpi-top">
        <div>
          <div class="kpi-label">High-Risk Customers</div>
          <div class="kpi-value">{hrc_fmt}</div>
        </div>
        <div class="kpi-icon">⚠️</div>
      </div>
      <div class="kpi-sub">Rated high or critical risk</div>
    </div>

  </div><!-- /kpi-grid -->

  <!-- CHART ROW 1 -->
  <div class="section-title">Volume & Distribution</div>
  <div class="chart-row chart-row-60-40" style="margin-bottom:20px;">

    <div class="chart-card">
      <div class="chart-card-title">📈 Transaction Volume &amp; Alert Frequency</div>
      <div class="chart-card-subtitle">Weekly transaction count vs. alert count — last 52 weeks</div>
      <div class="chart-wrapper" style="height:280px;">
        <canvas id="chartVolume"></canvas>
      </div>
    </div>

    <div class="chart-card">
      <div class="chart-card-title">🎯 Alert Severity Distribution</div>
      <div class="chart-card-subtitle">Breakdown of all {format_number(data['total_alerts'])} alerts by severity level</div>
      <div class="chart-wrapper" style="height:280px;">
        <canvas id="chartSeverity"></canvas>
      </div>
    </div>

  </div>

  <!-- CHART ROW 2 -->
  <div class="section-title">Rule Performance &amp; Geography</div>
  <div class="chart-row chart-row-50-50" style="margin-bottom:20px;">

    <div class="chart-card">
      <div class="chart-card-title">📋 Top 10 Rules by Alert Count</div>
      <div class="chart-card-subtitle">Most frequently triggered detection rules</div>
      <div class="chart-wrapper" style="height:320px;">
        <canvas id="chartRules"></canvas>
      </div>
    </div>

    <div class="chart-card">
      <div class="chart-card-title">🌍 Geographic Risk Distribution</div>
      <div class="chart-card-subtitle">Alert count by counterparty country — top 10</div>
      <div class="chart-wrapper" style="height:320px;">
        <canvas id="chartGeo"></canvas>
      </div>
    </div>

  </div>

  <!-- CHART ROW 3 -->
  <div class="section-title">Pipeline &amp; Effectiveness</div>
  <div class="chart-row chart-row-50-50" style="margin-bottom:20px;">

    <div class="chart-card">
      <div class="chart-card-title">🔀 Alert Status Pipeline</div>
      <div class="chart-card-subtitle">Distribution of alerts across workflow stages</div>
      <div class="chart-wrapper" id="funnelWrap" style="height:260px;"></div>
    </div>

    <div class="chart-card">
      <div class="chart-card-title">🎯 Rule Precision vs. Recall</div>
      <div class="chart-card-subtitle">Each point represents one detection rule — ideal quadrant: top-right</div>
      <div class="chart-wrapper" style="height:260px;">
        <canvas id="chartScatter"></canvas>
      </div>
    </div>

  </div>

  <!-- CHART ROW 4: Monthly Trend + Analyst Workload -->
  <div class="section-title">Monthly Trends &amp; Operations</div>
  <div class="two-col">

    <div class="chart-card">
      <div class="chart-card-title">📅 Monthly Transaction Volume</div>
      <div class="chart-card-subtitle">Transaction count &amp; total value — last 12 months</div>
      <div class="chart-wrapper" style="height:240px;">
        <canvas id="chartMonthly"></canvas>
      </div>
    </div>

    <div class="chart-card">
      <div class="chart-card-title">👤 Analyst Workload &amp; Outcomes</div>
      <div class="chart-card-subtitle">Alert assignments and resolution outcomes by analyst</div>
      <div class="chart-wrapper" style="height:240px;">
        <canvas id="chartWorkload"></canvas>
      </div>
    </div>

  </div>

  <!-- RULES PERFORMANCE TABLE -->
  <div class="section-title">Detection Rule Performance</div>
  <div class="rules-table-card">
    <div class="table-header">
      <div class="table-title">⚙️ All Detection Rules — Precision · Recall · F1</div>
      <div class="table-hint">🖱️ Click headers to sort</div>
    </div>
    <div class="table-scroll">
      <table id="rulesTable">
        <thead>
          <tr>
            <th onclick="sortRules(0)" data-col="0">Rule Name <span class="sort-icon">⇅</span></th>
            <th onclick="sortRules(1)" data-col="1">Category <span class="sort-icon">⇅</span></th>
            <th onclick="sortRules(2)" data-col="2">Total Alerts <span class="sort-icon">⇅</span></th>
            <th onclick="sortRules(3)" data-col="3">True Pos <span class="sort-icon">⇅</span></th>
            <th onclick="sortRules(4)" data-col="4">False Pos <span class="sort-icon">⇅</span></th>
            <th onclick="sortRules(5)" data-col="5">Precision <span class="sort-icon">⇅</span></th>
            <th onclick="sortRules(6)" data-col="6">Recall <span class="sort-icon">⇅</span></th>
            <th onclick="sortRules(7)" data-col="7">F1 Score <span class="sort-icon">⇅</span></th>
          </tr>
        </thead>
        <tbody id="rulesTableBody"></tbody>
      </table>
    </div>
  </div>

  <!-- RECENT ALERTS TABLE -->
  <div class="section-title">Recent Activity</div>
  <div class="table-card">
    <div class="table-header">
      <div class="table-title">🔔 Recent Alerts — Last 20</div>
      <div class="table-hint">🖱️ Click column headers to sort</div>
    </div>
    <div class="table-scroll">
      <table id="alertsTable">
        <thead>
          <tr>
            <th onclick="sortTable(0)" data-col="0">Alert ID <span class="sort-icon">⇅</span></th>
            <th onclick="sortTable(1)" data-col="1">Date <span class="sort-icon">⇅</span></th>
            <th onclick="sortTable(2)" data-col="2">Customer <span class="sort-icon">⇅</span></th>
            <th onclick="sortTable(3)" data-col="3" style="text-align:right;">Amount <span class="sort-icon">⇅</span></th>
            <th onclick="sortTable(4)" data-col="4">Rule Triggered <span class="sort-icon">⇅</span></th>
            <th onclick="sortTable(5)" data-col="5">Severity <span class="sort-icon">⇅</span></th>
            <th onclick="sortTable(6)" data-col="6">Status <span class="sort-icon">⇅</span></th>
            <th onclick="sortTable(7)" data-col="7">Assigned To <span class="sort-icon">⇅</span></th>
          </tr>
        </thead>
        <tbody id="alertsTableBody"></tbody>
      </table>
    </div>
  </div>

</main>

<!-- ===== FOOTER ===== -->
<footer class="footer">
  <span>TransactGuard AML</span> &nbsp;·&nbsp; Executive Intelligence Dashboard &nbsp;·&nbsp;
  Generated Mar 12, 2026 &nbsp;·&nbsp; Data: 12-month trailing window &nbsp;·&nbsp; Confidential — Internal Use Only
</footer>

<!-- ===== EMBEDDED DATA + CHARTS ===== -->
<script>
// ---- Embedded Data ----
const DATA = {{
  weeklyLabels:   {weekly_labels_js},
  weeklyTxn:      {weekly_txn_js},
  weeklyAlerts:   {weekly_alerts_js},
  severityDist:   {severity_js},
  topRulesLabels: {top_rules_labels_js},
  topRulesCounts: {top_rules_counts_js},
  geoLabels:      {geo_labels_js},
  geoCounts:      {geo_counts_js},
  geoRisks:       {geo_risks_js},
  funnelLabels:   {funnel_labels_js},
  funnelCounts:   {funnel_counts_js},
  scatter:        {scatter_js},
  recentAlerts:   {recent_js},
  sparklineTxn:   {sparkline_js},
  activeSeverity: {active_sev_js},
  rulesTable:     {rules_table_js},
  monthlyLabels:  {monthly_labels_js},
  monthlyCounts:  {monthly_counts_js},
  monthlyAmounts: {monthly_amounts_js},
  workload:       {workload_js},
  customerRisk:   {customer_risk_js},
}};

// ---- Shared Chart.js defaults ----
Chart.defaults.color = '#8b949e';
Chart.defaults.font.family = "'Inter', sans-serif";
Chart.defaults.font.size = 12;

const gridColor = 'rgba(48,54,61,0.7)';
const tickColor = '#6e7681';

// ========================
// KPI: Sparkline (total txn)
// ========================
(function () {{
  const ctx = document.getElementById('sparkTxn').getContext('2d');
  const labels = DATA.sparklineTxn.map((_, i) => `W${{i+1}}`);
  new Chart(ctx, {{
    type: 'line',
    data: {{
      labels,
      datasets: [{{
        data: DATA.sparklineTxn,
        borderColor: '#58a6ff',
        borderWidth: 2,
        pointRadius: 0,
        fill: true,
        backgroundColor: 'rgba(88,166,255,0.12)',
        tension: 0.4,
      }}]
    }},
    options: {{
      responsive: true,
      maintainAspectRatio: false,
      animation: {{ duration: 1200, easing: 'easeInOutQuart' }},
      plugins: {{ legend: {{ display: false }}, tooltip: {{ enabled: false }} }},
      scales: {{ x: {{ display: false }}, y: {{ display: false }} }},
    }}
  }});
}})();

// ========================
// KPI: Severity bar + legend
// ========================
(function () {{
  const sev = DATA.activeSeverity;
  const total = (sev.critical||0)+(sev.high||0)+(sev.medium||0)+(sev.low||0);
  const bar = document.getElementById('sevBar');
  const legend = document.getElementById('sevLegend');

  const items = [
    {{ key:'critical', color:'#f85149', label:'Critical' }},
    {{ key:'high',     color:'#f0883e', label:'High' }},
    {{ key:'medium',   color:'#d29922', label:'Medium' }},
    {{ key:'low',      color:'#3fb950', label:'Low' }},
  ];

  items.forEach(item => {{
    const cnt = sev[item.key] || 0;
    const pct = total > 0 ? (cnt / total * 100).toFixed(1) : 0;
    if (cnt > 0) {{
      const seg = document.createElement('div');
      seg.className = 'sev-seg';
      seg.style.width = pct + '%';
      seg.style.background = item.color;
      seg.title = `${{item.label}}: ${{cnt}}`;
      bar.appendChild(seg);
    }}
    const li = document.createElement('div');
    li.className = 'sev-legend-item';
    li.innerHTML = `<div class="sev-dot" style="background:${{item.color}}"></div>${{item.label}}: <strong style="color:#e6edf3">${{cnt}}</strong>`;
    legend.appendChild(li);
  }});
}})();

// ========================
// Chart 1: Volume Line (dual-axis)
// ========================
(function () {{
  const ctx = document.getElementById('chartVolume').getContext('2d');

  // Shorten labels: show every 4th week label
  const labels = DATA.weeklyLabels.map((w, i) => {{
    if (i % 4 === 0) {{
      const [yr, wk] = w.split('-');
      return `W${{parseInt(wk)}} '${{yr.slice(2)}}`;
    }}
    return '';
  }});

  new Chart(ctx, {{
    type: 'line',
    data: {{
      labels,
      datasets: [
        {{
          label: 'Transactions',
          data: DATA.weeklyTxn,
          borderColor: '#58a6ff',
          backgroundColor: 'rgba(88,166,255,0.08)',
          borderWidth: 2.5,
          pointRadius: 0,
          pointHoverRadius: 4,
          fill: true,
          tension: 0.4,
          yAxisID: 'yTxn',
        }},
        {{
          label: 'Alerts',
          data: DATA.weeklyAlerts,
          borderColor: '#f78166',
          backgroundColor: 'rgba(247,129,102,0.06)',
          borderWidth: 2,
          pointRadius: 0,
          pointHoverRadius: 4,
          fill: true,
          tension: 0.4,
          yAxisID: 'yAlerts',
          borderDash: [4, 3],
        }},
      ]
    }},
    options: {{
      responsive: true,
      maintainAspectRatio: false,
      animation: {{ duration: 1400, easing: 'easeInOutQuart' }},
      interaction: {{ mode: 'index', intersect: false }},
      plugins: {{
        legend: {{
          position: 'top',
          align: 'end',
          labels: {{ usePointStyle: true, pointStyleWidth: 16, padding: 20, color: '#8b949e' }}
        }},
        tooltip: {{
          backgroundColor: '#1c2128',
          borderColor: '#30363d',
          borderWidth: 1,
          titleColor: '#e6edf3',
          bodyColor: '#8b949e',
          padding: 12,
        }}
      }},
      scales: {{
        x: {{
          grid: {{ color: gridColor, drawBorder: false }},
          ticks: {{ color: tickColor, maxRotation: 0 }},
          border: {{ display: false }},
        }},
        yTxn: {{
          position: 'left',
          grid: {{ color: gridColor, drawBorder: false }},
          ticks: {{ color: '#58a6ff', callback: v => v >= 1000 ? (v/1000).toFixed(1)+'k' : v }},
          border: {{ display: false }},
          title: {{ display: true, text: 'Transactions', color: '#58a6ff', font: {{ size: 11 }} }},
        }},
        yAlerts: {{
          position: 'right',
          grid: {{ drawOnChartArea: false }},
          ticks: {{ color: '#f78166' }},
          border: {{ display: false }},
          title: {{ display: true, text: 'Alerts', color: '#f78166', font: {{ size: 11 }} }},
        }},
      }}
    }}
  }});
}})();

// ========================
// Chart 2: Severity Donut
// ========================
(function () {{
  const ctx = document.getElementById('chartSeverity').getContext('2d');
  const s = DATA.severityDist;
  const total = s.critical + s.high + s.medium + s.low;

  new Chart(ctx, {{
    type: 'doughnut',
    data: {{
      labels: ['Critical', 'High', 'Medium', 'Low'],
      datasets: [{{
        data: [s.critical, s.high, s.medium, s.low],
        backgroundColor: ['#f85149', '#f0883e', '#d29922', '#3fb950'],
        borderColor: '#161b22',
        borderWidth: 3,
        hoverBorderWidth: 0,
        hoverOffset: 6,
      }}]
    }},
    options: {{
      responsive: true,
      maintainAspectRatio: false,
      cutout: '68%',
      animation: {{ duration: 1200, easing: 'easeInOutQuart' }},
      plugins: {{
        legend: {{
          position: 'right',
          labels: {{
            usePointStyle: true,
            pointStyleWidth: 10,
            padding: 18,
            color: '#8b949e',
            generateLabels(chart) {{
              const d = chart.data;
              return d.labels.map((label, i) => {{
                const val = d.datasets[0].data[i];
                const pct = ((val/total)*100).toFixed(1);
                return {{
                  text: `${{label}}  ${{val.toLocaleString()}}  (${{pct}}%)`,
                  fillStyle: d.datasets[0].backgroundColor[i],
                  strokeStyle: d.datasets[0].backgroundColor[i],
                  pointStyle: 'circle',
                  index: i,
                }};
              }});
            }}
          }}
        }},
        tooltip: {{
          backgroundColor: '#1c2128',
          borderColor: '#30363d',
          borderWidth: 1,
          titleColor: '#e6edf3',
          bodyColor: '#8b949e',
          callbacks: {{
            label: ctx => ` ${{ctx.label}}: ${{ctx.parsed.toLocaleString()}} (${{((ctx.parsed/total)*100).toFixed(1)}}%)`
          }}
        }}
      }}
    }}
  }});
}})();

// ========================
// Chart 3: Top Rules Bar
// ========================
(function () {{
  const ctx = document.getElementById('chartRules').getContext('2d');
  const labels = DATA.topRulesLabels;
  const counts = DATA.topRulesCounts;

  // Generate blue gradient shades
  const colors = counts.map((_, i) => `rgba(88,166,255,${{1 - i * 0.06}})`);

  new Chart(ctx, {{
    type: 'bar',
    data: {{
      labels,
      datasets: [{{
        label: 'Alert Count',
        data: counts,
        backgroundColor: colors,
        borderColor: 'rgba(88,166,255,0.6)',
        borderWidth: 1,
        borderRadius: 4,
        borderSkipped: false,
      }}]
    }},
    options: {{
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      animation: {{ duration: 1200, easing: 'easeInOutQuart' }},
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          backgroundColor: '#1c2128',
          borderColor: '#30363d',
          borderWidth: 1,
          titleColor: '#e6edf3',
          bodyColor: '#8b949e',
          callbacks: {{ label: ctx => ` Alerts: ${{ctx.parsed.x.toLocaleString()}}` }}
        }}
      }},
      scales: {{
        x: {{
          grid: {{ color: gridColor, drawBorder: false }},
          ticks: {{ color: tickColor }},
          border: {{ display: false }},
        }},
        y: {{
          grid: {{ display: false }},
          ticks: {{ color: '#e6edf3', font: {{ size: 11 }} }},
          border: {{ display: false }},
        }}
      }}
    }}
  }});
}})();

// ========================
// Chart 4: Geographic Bar
// ========================
(function () {{
  const ctx = document.getElementById('chartGeo').getContext('2d');

  const riskColorMap = {{
    'low':      'rgba(63,185,80,0.75)',
    'medium':   'rgba(210,153,34,0.75)',
    'high':     'rgba(240,136,62,0.75)',
    'critical': 'rgba(248,81,73,0.75)',
    'unknown':  'rgba(110,118,129,0.75)',
  }};

  const colors = DATA.geoRisks.map(r => riskColorMap[r] || riskColorMap.unknown);

  new Chart(ctx, {{
    type: 'bar',
    data: {{
      labels: DATA.geoLabels,
      datasets: [{{
        label: 'Alert Count',
        data: DATA.geoCounts,
        backgroundColor: colors,
        borderColor: colors.map(c => c.replace('0.75', '1')),
        borderWidth: 1,
        borderRadius: 4,
        borderSkipped: false,
      }}]
    }},
    options: {{
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      animation: {{ duration: 1200, easing: 'easeInOutQuart' }},
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          backgroundColor: '#1c2128',
          borderColor: '#30363d',
          borderWidth: 1,
          titleColor: '#e6edf3',
          bodyColor: '#8b949e',
          callbacks: {{
            label: (ctx) => {{
              const risk = DATA.geoRisks[ctx.dataIndex];
              return ` Alerts: ${{ctx.parsed.x}} | Risk: ${{risk}}`;
            }}
          }}
        }}
      }},
      scales: {{
        x: {{
          grid: {{ color: gridColor, drawBorder: false }},
          ticks: {{ color: tickColor }},
          border: {{ display: false }},
        }},
        y: {{
          grid: {{ display: false }},
          ticks: {{ color: '#e6edf3', font: {{ size: 12 }} }},
          border: {{ display: false }},
        }}
      }}
    }}
  }});
}})();

// ========================
// Chart 5: Alert Funnel (custom HTML bars)
// ========================
(function () {{
  const wrap = document.getElementById('funnelWrap');
  const maxVal = Math.max(...DATA.funnelCounts);
  const total = DATA.funnelCounts.reduce((a, b) => a + b, 0);

  const colors = ['#58a6ff', '#d29922', '#f85149', '#3fb950', '#8b949e'];

  const container = document.createElement('div');
  container.className = 'funnel-container';
  container.style.paddingTop = '12px';

  DATA.funnelLabels.forEach((label, i) => {{
    const val = DATA.funnelCounts[i];
    const pct = total > 0 ? ((val / total) * 100).toFixed(1) : 0;
    const barW = maxVal > 0 ? ((val / maxVal) * 100) : 0;

    const item = document.createElement('div');
    item.className = 'funnel-item';
    item.innerHTML = `
      <div class="funnel-label">${{label}}</div>
      <div class="funnel-bar-wrap">
        <div class="funnel-bar" style="width:${{barW}}%;background:${{colors[i]}};transition-delay:${{i*80}}ms">
          ${{val.toLocaleString()}}
        </div>
      </div>
      <div class="funnel-pct">${{pct}}%</div>
    `;
    container.appendChild(item);
  }});

  wrap.appendChild(container);
}})();

// ========================
// Chart 6: Scatter — Precision vs Recall
// ========================
(function () {{
  const ctx = document.getElementById('chartScatter').getContext('2d');

  const points = DATA.scatter.map(r => ({{ x: r.precision, y: r.recall, name: r.name, abbr: r.abbr, alerts: r.alerts }}));

  new Chart(ctx, {{
    type: 'scatter',
    data: {{
      datasets: [{{
        label: 'Rules',
        data: points,
        backgroundColor: 'rgba(88,166,255,0.7)',
        borderColor: '#58a6ff',
        borderWidth: 1.5,
        pointRadius: 8,
        pointHoverRadius: 11,
      }}]
    }},
    options: {{
      responsive: true,
      maintainAspectRatio: false,
      animation: {{ duration: 1200, easing: 'easeInOutQuart' }},
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          backgroundColor: '#1c2128',
          borderColor: '#30363d',
          borderWidth: 1,
          titleColor: '#e6edf3',
          bodyColor: '#8b949e',
          callbacks: {{
            title: (items) => items[0].raw.name,
            label: (item) => [
              ` Precision: ${{item.raw.x.toFixed(3)}}`,
              ` Recall: ${{item.raw.y.toFixed(3)}}`,
              ` Alerts: ${{item.raw.alerts.toLocaleString()}}`,
            ]
          }}
        }},
        afterDraw: undefined,
      }},
      scales: {{
        x: {{
          min: 0, max: 1,
          title: {{ display: true, text: 'Precision', color: '#8b949e', font: {{ size: 11 }} }},
          grid: {{ color: gridColor, drawBorder: false }},
          ticks: {{ color: tickColor }},
          border: {{ display: false }},
        }},
        y: {{
          min: 0, max: 1,
          title: {{ display: true, text: 'Recall', color: '#8b949e', font: {{ size: 11 }} }},
          grid: {{ color: gridColor, drawBorder: false }},
          ticks: {{ color: tickColor }},
          border: {{ display: false }},
        }}
      }}
    }},
    plugins: [{{
      id: 'quadrantLines',
      afterDraw(chart) {{
        const {{ ctx, chartArea, scales }} = chart;
        const xMid = scales.x.getPixelForValue(0.5);
        const yMid = scales.y.getPixelForValue(0.5);

        ctx.save();
        ctx.strokeStyle = 'rgba(110,118,129,0.35)';
        ctx.lineWidth = 1;
        ctx.setLineDash([4, 4]);

        ctx.beginPath();
        ctx.moveTo(xMid, chartArea.top);
        ctx.lineTo(xMid, chartArea.bottom);
        ctx.stroke();

        ctx.beginPath();
        ctx.moveTo(chartArea.left, yMid);
        ctx.lineTo(chartArea.right, yMid);
        ctx.stroke();

        ctx.restore();

        // Quadrant labels
        ctx.save();
        ctx.font = "10px 'Inter', sans-serif";
        ctx.fillStyle = 'rgba(110,118,129,0.5)';
        ctx.fillText('Low Prec / High Rec', chartArea.left + 6, chartArea.top + 14);
        ctx.fillText('High Prec / High Rec', xMid + 6, chartArea.top + 14);
        ctx.fillText('Low Prec / Low Rec', chartArea.left + 6, chartArea.bottom - 6);
        ctx.fillText('High Prec / Low Rec', xMid + 6, chartArea.bottom - 6);
        ctx.restore();

        // Draw abbreviation labels on points
        chart.data.datasets[0].data.forEach((pt, i) => {{
          const x = scales.x.getPixelForValue(pt.x);
          const y = scales.y.getPixelForValue(pt.y);
          ctx.save();
          ctx.font = "bold 9px 'Inter', sans-serif";
          ctx.fillStyle = '#e6edf3';
          ctx.textAlign = 'center';
          ctx.fillText(pt.abbr, x, y + 18);
          ctx.restore();
        }});
      }}
    }}]
  }});
}})();

// ========================
// Chart 7: Monthly Volume (bar + line)
// ========================
(function () {{
  const ctx = document.getElementById('chartMonthly').getContext('2d');
  const labels = DATA.monthlyLabels.map(m => {{
    const [yr, mo] = m.split('-');
    const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    return months[parseInt(mo)-1] + ' \'' + yr.slice(2);
  }});

  new Chart(ctx, {{
    type: 'bar',
    data: {{
      labels,
      datasets: [
        {{
          type: 'bar',
          label: 'Transaction Count',
          data: DATA.monthlyCounts,
          backgroundColor: 'rgba(88,166,255,0.25)',
          borderColor: 'rgba(88,166,255,0.7)',
          borderWidth: 1,
          borderRadius: 3,
          yAxisID: 'yCnt',
          order: 2,
        }},
        {{
          type: 'line',
          label: 'Total Value ($)',
          data: DATA.monthlyAmounts,
          borderColor: '#3fb950',
          backgroundColor: 'rgba(63,185,80,0.06)',
          borderWidth: 2.5,
          pointRadius: 3,
          pointHoverRadius: 5,
          fill: true,
          tension: 0.3,
          yAxisID: 'yVal',
          order: 1,
        }},
      ]
    }},
    options: {{
      responsive: true,
      maintainAspectRatio: false,
      animation: {{ duration: 1200, easing: 'easeInOutQuart' }},
      interaction: {{ mode: 'index', intersect: false }},
      plugins: {{
        legend: {{
          position: 'top',
          align: 'end',
          labels: {{ usePointStyle: true, pointStyleWidth: 12, padding: 16, color: '#8b949e', font: {{ size: 11 }} }}
        }},
        tooltip: {{
          backgroundColor: '#1c2128', borderColor: '#30363d', borderWidth: 1,
          titleColor: '#e6edf3', bodyColor: '#8b949e', padding: 10,
          callbacks: {{
            label: (ctx) => {{
              if (ctx.datasetIndex === 0) return ` Count: ${{ctx.parsed.y.toLocaleString()}}`;
              return ` Value: $${{(ctx.parsed.y/1000000).toFixed(2)}}M`;
            }}
          }}
        }}
      }},
      scales: {{
        x: {{
          grid: {{ color: gridColor, drawBorder: false }},
          ticks: {{ color: tickColor, font: {{ size: 11 }} }},
          border: {{ display: false }},
        }},
        yCnt: {{
          position: 'left', grid: {{ color: gridColor, drawBorder: false }},
          ticks: {{ color: '#58a6ff', font: {{ size: 10 }}, callback: v => v >= 1000 ? (v/1000).toFixed(1)+'k' : v }},
          border: {{ display: false }},
        }},
        yVal: {{
          position: 'right', grid: {{ drawOnChartArea: false }},
          ticks: {{ color: '#3fb950', font: {{ size: 10 }}, callback: v => '$'+(v/1000000).toFixed(1)+'M' }},
          border: {{ display: false }},
        }},
      }}
    }}
  }});
}})();

// ========================
// Chart 8: Analyst Workload (stacked bar)
// ========================
(function () {{
  const ctx = document.getElementById('chartWorkload').getContext('2d');
  const analysts = DATA.workload.map(w => w.analyst === 'Unassigned' ? '⚠ Unassigned' : w.analyst);
  const totals  = DATA.workload.map(w => w.total - w.tp - w.fp);  // active (not yet closed)
  const tps     = DATA.workload.map(w => w.tp);
  const fps     = DATA.workload.map(w => w.fp);

  new Chart(ctx, {{
    type: 'bar',
    data: {{
      labels: analysts,
      datasets: [
        {{
          label: 'Active',
          data: totals,
          backgroundColor: 'rgba(88,166,255,0.6)',
          borderRadius: 3,
          stack: 'workload',
        }},
        {{
          label: 'True Positive',
          data: tps,
          backgroundColor: 'rgba(63,185,80,0.7)',
          borderRadius: 3,
          stack: 'workload',
        }},
        {{
          label: 'False Positive',
          data: fps,
          backgroundColor: 'rgba(110,118,129,0.5)',
          borderRadius: 3,
          stack: 'workload',
        }},
      ]
    }},
    options: {{
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      animation: {{ duration: 1200, easing: 'easeInOutQuart' }},
      plugins: {{
        legend: {{
          position: 'top', align: 'end',
          labels: {{ usePointStyle: true, pointStyleWidth: 10, padding: 14, color: '#8b949e', font: {{ size: 11 }} }}
        }},
        tooltip: {{
          backgroundColor: '#1c2128', borderColor: '#30363d', borderWidth: 1,
          titleColor: '#e6edf3', bodyColor: '#8b949e', padding: 10,
        }},
      }},
      scales: {{
        x: {{
          stacked: true,
          grid: {{ color: gridColor, drawBorder: false }},
          ticks: {{ color: tickColor }},
          border: {{ display: false }},
        }},
        y: {{
          stacked: true,
          grid: {{ display: false }},
          ticks: {{ color: '#e6edf3', font: {{ size: 11 }} }},
          border: {{ display: false }},
        }},
      }}
    }}
  }});
}})();

// ========================
// Rules Performance Table
// ========================
(function () {{
  const tbody = document.getElementById('rulesTableBody');
  let currentRules = [...DATA.rulesTable];
  let sortCol = -1;
  let sortAsc = true;

  function catBadge(cat) {{
    return `<span class="cat-badge cat-${{cat}}">${{cat}}</span>`;
  }}

  function precBar(val) {{
    const pct = Math.round(val * 100);
    const color = val >= 0.7 ? '#3fb950' : val >= 0.5 ? '#d29922' : '#f78166';
    return `
      <div style="display:flex;align-items:center;gap:8px;">
        <div class="perf-bar-wrap"><div class="perf-bar" style="width:${{pct}}%;background:${{color}}"></div></div>
        <span style="color:${{color}};font-weight:600;font-size:11px;">${{val.toFixed(3)}}</span>
      </div>`;
  }}

  function f1Class(val) {{
    if (val >= 0.65) return 'f1-good';
    if (val >= 0.45) return 'f1-ok';
    return 'f1-poor';
  }}

  function renderRuleRows(rules) {{
    tbody.innerHTML = '';
    rules.forEach(r => {{
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td style="font-weight:500">${{r.rule_name}}</td>
        <td>${{catBadge(r.category)}}</td>
        <td style="text-align:right;font-family:monospace;font-weight:600">${{r.total_alerts.toLocaleString()}}</td>
        <td style="text-align:right;color:#3fb950;font-weight:600">${{r.true_pos.toLocaleString()}}</td>
        <td style="text-align:right;color:#8b949e">${{r.false_pos.toLocaleString()}}</td>
        <td>${{precBar(r.precision)}}</td>
        <td>${{precBar(r.recall)}}</td>
        <td style="text-align:center"><span class="${{f1Class(r.f1)}}">${{r.f1.toFixed(3)}}</span></td>
      `;
      tbody.appendChild(tr);
    }});
  }}

  renderRuleRows(currentRules);

  window.sortRules = function(colIdx) {{
    const th = document.querySelectorAll('#rulesTable thead th');
    th.forEach(t => t.classList.remove('sorted'));
    th[colIdx].classList.add('sorted');

    if (sortCol === colIdx) {{ sortAsc = !sortAsc; }}
    else {{ sortCol = colIdx; sortAsc = true; }}

    const keys = ['rule_name','category','total_alerts','true_pos','false_pos','precision','recall','f1'];
    const key = keys[colIdx];
    currentRules.sort((a, b) => {{
      let va = a[key]; let vb = b[key];
      if (typeof va === 'string') {{ va = va.toLowerCase(); vb = vb.toLowerCase(); }}
      if (va < vb) return sortAsc ? -1 : 1;
      if (va > vb) return sortAsc ? 1 : -1;
      return 0;
    }});

    th[colIdx].querySelector('.sort-icon').textContent = sortAsc ? '↑' : '↓';
    renderRuleRows(currentRules);
  }};
}})();

// ========================
// Recent Alerts Table
// ========================
(function () {{
  const tbody = document.getElementById('alertsTableBody');

  function severityBadge(sev) {{
    return `<span class="badge badge-${{sev}}">${{sev.charAt(0).toUpperCase() + sev.slice(1)}}</span>`;
  }}

  function statusBadge(st) {{
    const labels = {{ new: 'New', under_review: 'Under Review', escalated: 'Escalated',
                      true_positive: 'True Positive', false_positive: 'False Positive' }};
    return `<span class="badge badge-${{st}}">${{labels[st] || st}}</span>`;
  }}

  function renderRows(alerts) {{
    tbody.innerHTML = '';
    alerts.forEach(a => {{
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td class="td-id">${{a.alert_id}}</td>
        <td class="td-date">${{a.date}}</td>
        <td class="td-customer">${{a.customer}}</td>
        <td class="td-amount" style="text-align:right">${{a.amount}}</td>
        <td class="td-rule" title="${{a.rule}}">${{a.rule}}</td>
        <td>${{severityBadge(a.severity)}}</td>
        <td>${{statusBadge(a.status)}}</td>
        <td class="td-assigned">${{a.assigned_to}}</td>
      `;
      tbody.appendChild(tr);
    }});
  }}

  let currentData = [...DATA.recentAlerts];
  let sortCol = -1;
  let sortAsc = true;

  renderRows(currentData);

  // Expose sort globally
  window.sortTable = function(colIdx) {{
    const th = document.querySelectorAll('#alertsTable thead th');
    th.forEach(t => t.classList.remove('sorted'));
    th[colIdx].classList.add('sorted');

    if (sortCol === colIdx) {{ sortAsc = !sortAsc; }}
    else {{ sortCol = colIdx; sortAsc = true; }}

    const keys = ['alert_id','date','customer','amount','rule','severity','status','assigned_to'];
    const key = keys[colIdx];

    currentData.sort((a, b) => {{
      let va = a[key] || '';
      let vb = b[key] || '';
      // Numeric sort for amount
      if (key === 'amount') {{
        va = parseFloat(va.replace(/[$,]/g,'')) || 0;
        vb = parseFloat(vb.replace(/[$,]/g,'')) || 0;
      }}
      if (va < vb) return sortAsc ? -1 : 1;
      if (va > vb) return sortAsc ? 1 : -1;
      return 0;
    }});

    const icon = sortAsc ? '↑' : '↓';
    th[colIdx].querySelector('.sort-icon').textContent = icon;

    renderRows(currentData);
  }};
}})();
</script>
</body>
</html>"""
    return html


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"Connecting to: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    try:
        print("Extracting data...")
        data = fetch_all_data(conn)
    finally:
        conn.close()

    print("Building HTML...")
    html = build_html(data)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    size_kb = os.path.getsize(OUTPUT_PATH) / 1024
    print(f"Dashboard written: {OUTPUT_PATH}")
    print(f"File size: {size_kb:.1f} KB")

    if size_kb < 50:
        print("WARNING: File is smaller than expected (< 50 KB)")
    else:
        print("File size check PASSED (> 50 KB)")

    # Quick sanity summary
    print(f"\nKey metrics:")
    print(f"  Total transactions : {data['total_transactions']:,}")
    print(f"  Total alerts       : {data['total_alerts']:,}")
    print(f"  Active alerts      : {data['active_alerts']:,}")
    print(f"  Closure rate       : {data['closure_rate']}%")
    print(f"  Flagged amount     : {data['total_flagged_display']}")
    print(f"  High-risk customers: {data['high_risk_customers']:,}")


if __name__ == "__main__":
    main()
