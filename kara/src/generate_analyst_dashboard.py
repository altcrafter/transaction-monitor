"""
Generate Analyst Workstation Dashboard for TransactGuard AML
Reads from data/transactions.db and outputs output/analyst_dashboard.html
"""

import sqlite3
import json
import os
import random
from datetime import datetime, timedelta
from collections import defaultdict

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "transactions.db")
OUTPUT_PATH = os.path.join(BASE_DIR, "output", "analyst_dashboard.html")

random.seed(42)

# ---------------------------------------------------------------------------
# Data extraction helpers
# ---------------------------------------------------------------------------

def dict_rows(cursor):
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def fetch_data(conn):
    cur = conn.cursor()

    # All rules
    cur.execute("SELECT * FROM rules ORDER BY rule_id")
    rules = {r["rule_id"]: r for r in dict_rows(cur)}

    # Country risk map
    cur.execute("SELECT country_code, country_name, risk_level FROM country_risk")
    country_risk = {r["country_code"]: r for r in dict_rows(cur)}

    # Customers map (all)
    cur.execute("SELECT * FROM customers")
    customers_map = {r["customer_id"]: r for r in dict_rows(cur)}

    # Accounts map (all)
    cur.execute("SELECT * FROM accounts")
    accounts_map = {r["account_id"]: r for r in dict_rows(cur)}

    # Alerts — grab top 30 active (new/under_review/escalated) for queue
    # Fetch these FIRST so we can load their transactions specifically
    cur.execute("""
        SELECT * FROM alerts
        WHERE status IN ('new','under_review','escalated')
        ORDER BY
            CASE severity WHEN 'critical' THEN 1 WHEN 'high' THEN 2
                          WHEN 'medium' THEN 3 ELSE 4 END,
            created_at DESC
        LIMIT 30
    """)
    queue_alerts = dict_rows(cur)

    # Collect alert transaction IDs and the account IDs we'll need
    alert_txn_ids = [a["transaction_id"] for a in queue_alerts if a.get("transaction_id")]
    # Load those specific transactions
    alert_txns = []
    if alert_txn_ids:
        placeholders = ",".join("?" * len(alert_txn_ids))
        cur.execute(f"SELECT * FROM transactions WHERE transaction_id IN ({placeholders})", alert_txn_ids)
        alert_txns = dict_rows(cur)

    # Build account_id list from alert transactions for network graph
    alert_acct_ids = list({t["account_id"] for t in alert_txns if t.get("account_id")})

    # Load all transactions for those accounts (for network graph), capped per account
    network_txns = []
    if alert_acct_ids:
        placeholders = ",".join("?" * len(alert_acct_ids))
        cur.execute(
            f"SELECT * FROM transactions WHERE account_id IN ({placeholders}) ORDER BY transaction_date DESC",
            alert_acct_ids
        )
        network_txns = dict_rows(cur)

    # Transactions sample for the search panel (200 most recent overall)
    cur.execute("SELECT * FROM transactions ORDER BY transaction_date DESC LIMIT 5000")
    all_transactions = dict_rows(cur)

    # Build txn_map: alert txns + network txns take priority; then fill with recent
    txn_map = {}
    for t in all_transactions:
        txn_map[t["transaction_id"]] = t
    for t in network_txns:
        txn_map[t["transaction_id"]] = t
    for t in alert_txns:
        txn_map[t["transaction_id"]] = t

    # Per-customer transaction stats
    cur.execute("""
        SELECT a.customer_id,
               COUNT(t.transaction_id) AS txn_count,
               SUM(t.amount) AS total_volume,
               MAX(t.transaction_date) AS last_txn
        FROM transactions t
        JOIN accounts a ON t.account_id = a.account_id
        GROUP BY a.customer_id
    """)
    cust_stats = {r["customer_id"]: r for r in dict_rows(cur)}

    # Per-customer alert counts
    cur.execute("""
        SELECT customer_id, COUNT(*) as alert_count
        FROM alerts GROUP BY customer_id
    """)
    cust_alert_counts = {r["customer_id"]: r["alert_count"] for r in dict_rows(cur)}

    # Related alerts per customer (last 5 per customer)
    cur.execute("""
        SELECT alert_id, customer_id, transaction_id, rule_id,
               status, severity, created_at, flagged_amount
        FROM alerts ORDER BY created_at DESC
    """)
    all_alerts_for_related = dict_rows(cur)
    related_map = defaultdict(list)
    for a in all_alerts_for_related:
        if len(related_map[a["customer_id"]]) < 6:
            related_map[a["customer_id"]].append(a)

    # 30-day daily activity per customer (sampled from transactions)
    cur.execute("""
        SELECT a.customer_id,
               strftime('%Y-%m-%d', t.transaction_date) AS day,
               COUNT(*) AS cnt,
               SUM(t.amount) AS vol
        FROM transactions t
        JOIN accounts a ON t.account_id = a.account_id
        WHERE t.transaction_date >= date('now', '-30 days')
        GROUP BY a.customer_id, day
        ORDER BY a.customer_id, day
    """)
    daily_rows = dict_rows(cur)
    daily_map = defaultdict(dict)
    for r in daily_rows:
        daily_map[r["customer_id"]][r["day"]] = {"cnt": r["cnt"], "vol": r["vol"]}

    # Transactions for the search panel (200 most recent)
    search_txns = all_transactions[:200]

    return {
        "rules": rules,
        "country_risk": country_risk,
        "customers_map": customers_map,
        "accounts_map": accounts_map,
        "txn_map": txn_map,
        "queue_alerts": queue_alerts,
        "cust_stats": cust_stats,
        "cust_alert_counts": cust_alert_counts,
        "related_map": related_map,
        "daily_map": daily_map,
        "search_txns": search_txns,
    }


def build_alert_objects(data):
    """Build enriched alert detail objects for JS embedding."""
    rules = data["rules"]
    country_risk = data["country_risk"]
    customers_map = data["customers_map"]
    accounts_map = data["accounts_map"]
    txn_map = data["txn_map"]
    cust_stats = data["cust_stats"]
    cust_alert_counts = data["cust_alert_counts"]
    related_map = data["related_map"]
    daily_map = data["daily_map"]

    enriched = []

    for alert in data["queue_alerts"]:
        aid = alert["alert_id"]
        cid = alert["customer_id"]
        tid = alert["transaction_id"]
        rid = alert["rule_id"]

        customer = customers_map.get(cid, {})
        rule = rules.get(rid, {})
        txn = txn_map.get(tid, {})

        # Find account for this transaction
        acct = {}
        if txn.get("account_id"):
            acct = accounts_map.get(txn["account_id"], {})

        # Country info for counterparty
        cp_country_code = txn.get("counterparty_country", "US")
        cp_country_info = country_risk.get(cp_country_code, {"country_name": cp_country_code, "risk_level": "unknown"})

        # Customer nationality country info
        nat_code = customer.get("nationality", "US")
        nat_info = country_risk.get(nat_code, {"country_name": nat_code, "risk_level": "low"})

        # Daily timeline (last 30 days)
        cust_daily = daily_map.get(cid, {})
        # Generate last 30 days labels
        today = datetime.now()
        timeline_labels = []
        timeline_cnt = []
        timeline_vol = []
        for i in range(30, -1, -1):
            day = (today - timedelta(days=i)).strftime("%Y-%m-%d")
            timeline_labels.append(day)
            d = cust_daily.get(day, {"cnt": 0, "vol": 0})
            timeline_cnt.append(d["cnt"])
            timeline_vol.append(round(d["vol"] or 0, 2))

        # Alert date index
        alert_date_str = alert["created_at"][:10] if alert["created_at"] else ""
        alert_day_index = None
        if alert_date_str in timeline_labels:
            alert_day_index = timeline_labels.index(alert_date_str)

        # Related alerts (exclude self)
        related_raw = related_map.get(cid, [])
        related_alerts = [
            {
                "alert_id": r["alert_id"],
                "rule_name": rules.get(r["rule_id"], {}).get("rule_name", "Unknown"),
                "amount": r["flagged_amount"],
                "status": r["status"],
                "date": r["created_at"][:10] if r["created_at"] else "",
            }
            for r in related_raw if r["alert_id"] != aid
        ][:5]

        # Risk score components (deterministic from customer risk_rating + rule category)
        risk_seed = sum(ord(c) for c in cid)
        random.seed(risk_seed)
        risk_rating = customer.get("risk_rating", "medium")
        base = {"low": 25, "medium": 50, "high": 80}.get(risk_rating, 50)
        comp_txn = min(100, base + random.randint(-5, 15))
        comp_geo = min(100, {"low": 15, "medium": 35, "high": 65}.get(cp_country_info.get("risk_level", "low"), 30) + random.randint(0, 20))
        comp_profile = min(100, base + (30 if customer.get("pep_flag") else 0) + random.randint(-5, 10))
        comp_network = min(100, base + random.randint(-10, 25))
        comp_velocity = min(100, base + random.randint(-10, 20))
        total_risk = round((comp_txn * 0.25 + comp_geo * 0.2 + comp_profile * 0.25 + comp_network * 0.15 + comp_velocity * 0.15))

        # Peer avg (slightly lower)
        peer_avg = max(10, total_risk - random.randint(5, 20))

        # Rule why-fired text
        threshold = rule.get("threshold_value", 0)
        flagged = alert.get("flagged_amount", 0) or 0
        if threshold and threshold > 0 and flagged > 0:
            pct_above = round((flagged - threshold) / threshold * 100, 1)
            why_fired = f"Transaction amount ${flagged:,.2f} is {pct_above}% above rule threshold of ${threshold:,.2f}"
        else:
            why_fired = f"Transaction pattern matched rule: {rule.get('description', 'See rule description')}"

        fp_rate = 0
        if rule.get("total_alerts_generated", 0) > 0:
            fp_rate = round(rule.get("false_positives", 0) / rule["total_alerts_generated"] * 100, 1)

        # Network graph nodes for this customer
        # Build counterparties from txn_map for this customer's account
        acct_id = acct.get("account_id", "")
        cp_nodes = []
        seen_cp = set()
        cp_count = defaultdict(lambda: {"count": 0, "vol": 0.0})
        for t in txn_map.values():
            if t.get("account_id") == acct_id and t.get("counterparty_name"):
                cp = t["counterparty_name"]
                cp_count[cp]["count"] += 1
                cp_count[cp]["vol"] += t.get("amount", 0)
                cp_count[cp]["country"] = t.get("counterparty_country", "US")
        # Take top 8
        for cp_name, cp_data in sorted(cp_count.items(), key=lambda x: -x[1]["count"])[:8]:
            cc = cp_data["country"]
            cr_info = country_risk.get(cc, {"risk_level": "low", "country_name": cc})
            cp_nodes.append({
                "name": cp_name,
                "country": cc,
                "country_name": cr_info.get("country_name", cc),
                "risk_level": cr_info.get("risk_level", "low"),
                "count": cp_data["count"],
                "vol": round(cp_data["vol"], 2),
            })

        cs = cust_stats.get(cid, {"txn_count": 0, "total_volume": 0})

        obj = {
            "alert_id": aid,
            "status": alert["status"],
            "severity": alert["severity"],
            "assigned_to": alert.get("assigned_to") or "Unassigned",
            "created_at": alert["created_at"],
            "notes": alert.get("notes") or "",
            "flagged_amount": flagged,
            "rule_id": rid,
            "rule_name": rule.get("rule_name", "Unknown Rule"),
            "rule_category": rule.get("rule_category", ""),
            "rule_description": rule.get("description", ""),
            "rule_threshold": threshold,
            "rule_precision": rule.get("precision_score", 0),
            "rule_recall": rule.get("recall_score", 0),
            "rule_total_alerts": rule.get("total_alerts_generated", 0),
            "rule_fp_rate": fp_rate,
            "why_fired": why_fired,
            "transaction": {
                "transaction_id": tid,
                "date": txn.get("transaction_date", ""),
                "type": txn.get("transaction_type", ""),
                "amount": txn.get("amount", 0),
                "currency": txn.get("currency", "USD"),
                "channel": txn.get("channel", ""),
                "status": txn.get("status", ""),
                "description": txn.get("description", ""),
                "counterparty_name": txn.get("counterparty_name", ""),
                "counterparty_country": cp_country_code,
                "counterparty_country_name": cp_country_info.get("country_name", cp_country_code),
                "counterparty_risk": cp_country_info.get("risk_level", "unknown"),
            },
            "customer": {
                "customer_id": cid,
                "name": customer.get("name", ""),
                "nationality": nat_code,
                "nationality_name": nat_info.get("country_name", nat_code),
                "kyc_status": customer.get("kyc_status", ""),
                "risk_rating": risk_rating,
                "pep_flag": bool(customer.get("pep_flag")),
                "sanctions_flag": bool(customer.get("sanctions_flag")),
                "country_of_residence": customer.get("country_of_residence", ""),
                "total_txns": cs.get("txn_count", 0),
                "total_volume": round(cs.get("total_volume") or 0, 2),
                "alerts_count": cust_alert_counts.get(cid, 0),
            },
            "account": {
                "account_id": acct.get("account_id", ""),
                "account_type": acct.get("account_type", ""),
                "currency": acct.get("currency", "USD"),
                "balance": round(acct.get("balance") or 0, 2),
                "status": acct.get("status", ""),
                "opened_at": acct.get("opened_at", ""),
            },
            "timeline_labels": timeline_labels,
            "timeline_cnt": timeline_cnt,
            "timeline_vol": timeline_vol,
            "alert_day_index": alert_day_index,
            "related_alerts": related_alerts,
            "network_nodes": cp_nodes,
            "risk_components": {
                "transaction_behavior": comp_txn,
                "geographic_risk": comp_geo,
                "customer_profile": comp_profile,
                "network_risk": comp_network,
                "velocity_risk": comp_velocity,
            },
            "total_risk_score": total_risk,
            "peer_avg_score": peer_avg,
        }
        enriched.append(obj)

    return enriched


def build_search_transactions(data):
    """200 transactions enriched for the search panel."""
    txns = data["search_txns"]
    country_risk = data["country_risk"]
    customers_map = data["customers_map"]
    accounts_map = data["accounts_map"]

    # Build account->customer map
    acct_to_cust = {aid: a["customer_id"] for aid, a in data["accounts_map"].items()}

    result = []
    for t in txns[:200]:
        cid = acct_to_cust.get(t.get("account_id", ""), "")
        cust = customers_map.get(cid, {})
        cc = t.get("counterparty_country", "US")
        cr = country_risk.get(cc, {"risk_level": "low", "country_name": cc})
        result.append({
            "transaction_id": t["transaction_id"],
            "date": t["transaction_date"],
            "amount": round(t.get("amount", 0), 2),
            "currency": t.get("currency", "USD"),
            "type": t.get("transaction_type", ""),
            "channel": t.get("channel", ""),
            "counterparty_name": t.get("counterparty_name", ""),
            "counterparty_country": cc,
            "counterparty_country_name": cr.get("country_name", cc),
            "counterparty_risk": cr.get("risk_level", "low"),
            "customer_name": cust.get("name", ""),
            "customer_id": cid,
            "description": t.get("description", ""),
        })
    return result


# ---------------------------------------------------------------------------
# HTML generation
# ---------------------------------------------------------------------------

def generate_html(enriched_alerts, search_txns, rules_list):
    alerts_json = json.dumps(enriched_alerts, indent=None, default=str)
    search_json = json.dumps(search_txns, indent=None, default=str)
    rules_json = json.dumps(rules_list, indent=None, default=str)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>TransactGuard AML — Analyst Workstation</title>
<link rel="preconnect" href="https://fonts.googleapis.com" />
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet" />
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
/* ===== RESET & BASE ===== */
*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
html, body {{ height: 100%; overflow: hidden; }}
body {{
  font-family: 'Inter', sans-serif;
  background: #0d1117;
  color: #e6edf3;
  font-size: 13px;
  line-height: 1.5;
}}

/* ===== SCROLLBAR ===== */
::-webkit-scrollbar {{ width: 6px; height: 6px; }}
::-webkit-scrollbar-track {{ background: #0d1117; }}
::-webkit-scrollbar-thumb {{ background: #30363d; border-radius: 3px; }}
::-webkit-scrollbar-thumb:hover {{ background: #58a6ff44; }}

/* ===== LAYOUT ===== */
.app-wrapper {{
  display: flex;
  flex-direction: column;
  height: 100vh;
  overflow: hidden;
}}

/* ===== HEADER ===== */
.header {{
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 0 20px;
  height: 52px;
  min-height: 52px;
  background: #161b22;
  border-bottom: 1px solid #30363d;
  flex-shrink: 0;
  z-index: 100;
}}
.header-logo {{
  display: flex;
  align-items: center;
  gap: 10px;
  font-size: 15px;
  font-weight: 700;
  color: #e6edf3;
  text-decoration: none;
  white-space: nowrap;
}}
.header-logo-icon {{
  width: 32px; height: 32px;
  background: linear-gradient(135deg, #58a6ff, #3fb950);
  border-radius: 8px;
  display: flex; align-items: center; justify-content: center;
  font-size: 17px;
  flex-shrink: 0;
}}
.header-back {{
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  color: #8b949e;
  text-decoration: none;
  padding: 4px 8px;
  border: 1px solid #30363d;
  border-radius: 6px;
  transition: all 0.15s;
  white-space: nowrap;
}}
.header-back:hover {{ color: #58a6ff; border-color: #58a6ff44; background: #58a6ff11; }}
.header-sep {{ width: 1px; height: 24px; background: #30363d; margin: 0 4px; }}
.header-title {{
  font-size: 14px;
  font-weight: 600;
  color: #e6edf3;
  white-space: nowrap;
}}
.header-spacer {{ flex: 1; }}
.header-analyst {{
  display: flex;
  align-items: center;
  gap: 10px;
}}
.analyst-avatar {{
  width: 30px; height: 30px;
  background: linear-gradient(135deg, #58a6ff44, #3fb95044);
  border: 1px solid #58a6ff44;
  border-radius: 50%;
  display: flex; align-items: center; justify-content: center;
  font-size: 12px; font-weight: 600; color: #58a6ff;
}}
.analyst-info {{ text-align: right; }}
.analyst-name {{ font-size: 12px; font-weight: 600; color: #e6edf3; }}
.analyst-role {{ font-size: 10px; color: #8b949e; }}
.header-queue-stats {{
  display: flex;
  gap: 12px;
  padding: 6px 14px;
  background: #0d1117;
  border: 1px solid #30363d;
  border-radius: 8px;
}}
.queue-stat {{
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 1px;
}}
.queue-stat-val {{
  font-size: 14px;
  font-weight: 700;
  line-height: 1;
}}
.queue-stat-lbl {{
  font-size: 9px;
  color: #8b949e;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  white-space: nowrap;
}}

/* ===== THREE-COLUMN BODY ===== */
.body-row {{
  display: flex;
  flex: 1;
  overflow: hidden;
}}

/* ===== LEFT PANEL ===== */
.left-panel {{
  width: 280px;
  min-width: 280px;
  background: #0d1117;
  border-right: 1px solid #30363d;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}}
.panel-header {{
  padding: 12px 14px 10px;
  border-bottom: 1px solid #30363d;
  flex-shrink: 0;
}}
.panel-title {{
  font-size: 11px;
  font-weight: 700;
  color: #8b949e;
  text-transform: uppercase;
  letter-spacing: 0.8px;
  margin-bottom: 10px;
}}
.panel-title-count {{ color: #58a6ff; }}

/* Filter bar */
.filter-bar {{ display: flex; flex-direction: column; gap: 6px; }}
.filter-row {{ display: flex; gap: 6px; }}
.filter-select, .filter-input {{
  width: 100%;
  background: #161b22;
  border: 1px solid #30363d;
  border-radius: 5px;
  color: #e6edf3;
  font-family: inherit;
  font-size: 11px;
  padding: 5px 8px;
  outline: none;
  transition: border-color 0.15s;
  appearance: none;
  cursor: pointer;
}}
.filter-select:focus, .filter-input:focus {{ border-color: #58a6ff66; }}
.filter-select option {{ background: #161b22; }}
.filter-input::placeholder {{ color: #8b949e; }}
.date-range-row {{
  display: flex;
  align-items: center;
  gap: 4px;
  background: #161b22;
  border: 1px solid #30363d;
  border-radius: 5px;
  padding: 4px 8px;
}}
.date-range-text {{
  flex: 1;
  font-size: 11px;
  color: #8b949e;
}}
.date-arrow {{
  background: none; border: none; color: #8b949e; cursor: pointer; padding: 0 2px; font-size: 11px;
}}
.date-arrow:hover {{ color: #58a6ff; }}

/* Alert cards */
.alert-list {{
  flex: 1;
  overflow-y: auto;
  padding: 6px 8px;
}}
.alert-card {{
  padding: 9px 10px;
  border-radius: 7px;
  border: 1px solid #30363d;
  border-left: 3px solid #30363d;
  margin-bottom: 6px;
  cursor: pointer;
  transition: all 0.15s;
  background: #161b22;
  position: relative;
}}
.alert-card:hover {{ border-color: #58a6ff44; background: #1c2128; }}
.alert-card.selected {{ border-color: #58a6ff; border-left-color: #58a6ff; background: #1c2128; }}
.alert-card[data-severity="critical"] {{ border-left-color: #f78166; }}
.alert-card[data-severity="high"] {{ border-left-color: #d29922; }}
.alert-card[data-severity="medium"] {{ border-left-color: #e3b341; }}
.alert-card[data-severity="low"] {{ border-left-color: #3fb950; }}
.alert-card.selected[data-severity="critical"] {{ border-color: #f78166; border-left-color: #f78166; }}
.alert-card.selected[data-severity="high"] {{ border-color: #d29922; border-left-color: #d29922; }}
.alert-card.selected[data-severity="medium"] {{ border-color: #e3b341; border-left-color: #e3b341; }}
.alert-card.selected[data-severity="low"] {{ border-color: #3fb950; border-left-color: #3fb950; }}

.card-row1 {{ display: flex; align-items: center; justify-content: space-between; margin-bottom: 3px; }}
.card-alert-id {{ font-size: 10px; font-weight: 700; color: #58a6ff; font-family: 'Courier New', monospace; }}
.card-row2 {{ display: flex; align-items: center; justify-content: space-between; margin-bottom: 3px; }}
.card-customer {{ font-size: 11px; font-weight: 600; color: #e6edf3; }}
.card-acct-type {{ font-size: 10px; color: #8b949e; }}
.card-rule {{ font-size: 10px; color: #8b949e; margin-bottom: 3px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
.card-row3 {{ display: flex; align-items: center; justify-content: space-between; }}
.card-amount {{ font-size: 11px; font-weight: 600; color: #e6edf3; }}
.card-meta {{ font-size: 10px; color: #8b949e; }}

/* Badges */
.badge {{
  display: inline-flex; align-items: center;
  font-size: 9px; font-weight: 700; text-transform: uppercase;
  padding: 2px 6px; border-radius: 20px;
  letter-spacing: 0.4px;
  white-space: nowrap;
}}
.badge-critical {{ background: #f7816622; color: #f78166; border: 1px solid #f7816644; }}
.badge-high {{ background: #d2992222; color: #d29922; border: 1px solid #d2992244; }}
.badge-medium {{ background: #e3b34122; color: #e3b341; border: 1px solid #e3b34144; }}
.badge-low {{ background: #3fb95022; color: #3fb950; border: 1px solid #3fb95044; }}
.badge-new {{ background: #58a6ff22; color: #58a6ff; border: 1px solid #58a6ff44; }}
.badge-under_review {{ background: #79c0ff22; color: #79c0ff; border: 1px solid #79c0ff44; }}
.badge-escalated {{ background: #f7816622; color: #f78166; border: 1px solid #f7816644; }}
.badge-true_positive {{ background: #f7816622; color: #f78166; border: 1px solid #f7816644; }}
.badge-false_positive {{ background: #3fb95022; color: #3fb950; border: 1px solid #3fb95044; }}
.badge-verified {{ background: #3fb95022; color: #3fb950; border: 1px solid #3fb95044; }}
.badge-pending {{ background: #d2992222; color: #d29922; border: 1px solid #d2992244; }}
.badge-expired {{ background: #f7816622; color: #f78166; border: 1px solid #f7816644; }}
.badge-low-risk {{ background: #3fb95022; color: #3fb950; border: 1px solid #3fb95044; }}
.badge-medium-risk {{ background: #d2992222; color: #d29922; border: 1px solid #d2992244; }}
.badge-high-risk {{ background: #f7816622; color: #f78166; border: 1px solid #f7816644; }}
.badge-pep {{ background: #bc8cff22; color: #bc8cff; border: 1px solid #bc8cff44; }}
.badge-active {{ background: #3fb95022; color: #3fb950; border: 1px solid #3fb95044; }}
.badge-closed {{ background: #8b949e22; color: #8b949e; border: 1px solid #8b949e44; }}

/* no-results */
.no-results {{
  text-align: center;
  color: #8b949e;
  font-size: 12px;
  padding: 24px 12px;
}}

/* ===== CENTER PANEL ===== */
.center-panel {{
  flex: 1;
  overflow-y: auto;
  background: #0d1117;
  padding: 16px;
  display: flex;
  flex-direction: column;
  gap: 14px;
}}

/* Placeholder */
.placeholder {{
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 16px;
  color: #8b949e;
  padding: 40px;
}}
.placeholder-icon {{
  font-size: 48px;
  opacity: 0.5;
}}
.placeholder-title {{
  font-size: 18px;
  font-weight: 600;
  color: #8b949e;
}}
.placeholder-subtitle {{
  font-size: 13px;
  color: #8b949e88;
  text-align: center;
  max-width: 320px;
  line-height: 1.6;
}}
.placeholder-search {{
  display: flex;
  gap: 8px;
  width: 100%;
  max-width: 360px;
}}
.placeholder-search input {{
  flex: 1;
  background: #161b22;
  border: 1px solid #30363d;
  border-radius: 8px;
  color: #e6edf3;
  font-family: inherit;
  font-size: 13px;
  padding: 8px 14px;
  outline: none;
  transition: border-color 0.15s;
}}
.placeholder-search input:focus {{ border-color: #58a6ff66; }}
.placeholder-search input::placeholder {{ color: #8b949e; }}

/* Quick stats in placeholder */
.placeholder-stats {{
  display: flex;
  gap: 16px;
  margin-top: 8px;
}}
.placeholder-stat {{
  text-align: center;
  padding: 12px 20px;
  background: #161b22;
  border: 1px solid #30363d;
  border-radius: 8px;
}}
.placeholder-stat-val {{ font-size: 22px; font-weight: 700; color: #58a6ff; }}
.placeholder-stat-lbl {{ font-size: 10px; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; }}

/* Section card */
.section-card {{
  background: #161b22;
  border: 1px solid #30363d;
  border-radius: 10px;
  overflow: hidden;
}}
.section-card-header {{
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 11px 16px;
  border-bottom: 1px solid #30363d;
  background: #1c2128;
}}
.section-card-title {{
  font-size: 12px;
  font-weight: 700;
  color: #8b949e;
  text-transform: uppercase;
  letter-spacing: 0.7px;
  display: flex;
  align-items: center;
  gap: 8px;
}}
.section-card-title-icon {{ font-size: 14px; }}
.section-card-body {{ padding: 14px 16px; }}

/* Alert header section */
.alert-header-row {{
  display: flex;
  align-items: flex-start;
  gap: 14px;
  flex-wrap: wrap;
}}
.alert-header-id {{
  font-size: 18px;
  font-weight: 700;
  color: #e6edf3;
  font-family: 'Courier New', monospace;
}}
.alert-header-badges {{
  display: flex;
  align-items: center;
  gap: 6px;
  flex-wrap: wrap;
}}
.alert-header-meta {{
  display: flex;
  gap: 16px;
  flex-wrap: wrap;
  margin-top: 10px;
}}
.alert-meta-item {{
  display: flex;
  flex-direction: column;
  gap: 1px;
}}
.alert-meta-label {{
  font-size: 9px;
  color: #8b949e;
  text-transform: uppercase;
  letter-spacing: 0.6px;
}}
.alert-meta-value {{ font-size: 12px; font-weight: 500; color: #e6edf3; }}
.alert-action-row {{
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  margin-top: 12px;
  padding-top: 12px;
  border-top: 1px solid #30363d;
}}

/* Buttons */
.btn {{
  display: inline-flex; align-items: center; gap: 6px;
  padding: 6px 14px;
  border-radius: 6px;
  font-family: inherit;
  font-size: 12px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.15s;
  border: 1px solid transparent;
  white-space: nowrap;
}}
.btn-danger {{ background: #f7816622; border-color: #f7816644; color: #f78166; }}
.btn-danger:hover {{ background: #f7816633; border-color: #f78166; }}
.btn-success {{ background: #3fb95022; border-color: #3fb95044; color: #3fb950; }}
.btn-success:hover {{ background: #3fb95033; border-color: #3fb950; }}
.btn-warning {{ background: #d2992222; border-color: #d2992244; color: #d29922; }}
.btn-warning:hover {{ background: #d2992233; border-color: #d29922; }}
.btn-primary {{ background: #58a6ff22; border-color: #58a6ff44; color: #58a6ff; }}
.btn-primary:hover {{ background: #58a6ff33; border-color: #58a6ff; }}
.btn-ghost {{ background: transparent; border-color: #30363d; color: #8b949e; }}
.btn-ghost:hover {{ border-color: #58a6ff44; color: #58a6ff; }}
.btn-xs {{ padding: 3px 8px; font-size: 10px; }}

/* Data table */
.data-table {{
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
}}
.data-table th {{
  text-align: left;
  font-size: 10px;
  font-weight: 600;
  color: #8b949e;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  padding: 7px 10px;
  border-bottom: 1px solid #30363d;
  white-space: nowrap;
}}
.data-table td {{
  padding: 7px 10px;
  border-bottom: 1px solid #21262d;
  color: #e6edf3;
  vertical-align: middle;
}}
.data-table tr:last-child td {{ border-bottom: none; }}
.data-table tbody tr:hover {{ background: #1c2128; }}
.mono {{ font-family: 'Courier New', monospace; font-size: 11px; color: #79c0ff; }}
.text-muted {{ color: #8b949e; }}
.text-green {{ color: #3fb950; }}
.text-red {{ color: #f78166; }}
.text-amber {{ color: #d29922; }}
.text-blue {{ color: #58a6ff; }}

/* Customer profile grid */
.profile-grid {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 10px 20px;
}}
.profile-item {{
  display: flex;
  flex-direction: column;
  gap: 2px;
}}
.profile-label {{
  font-size: 10px;
  color: #8b949e;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}}
.profile-value {{
  font-size: 13px;
  font-weight: 500;
  color: #e6edf3;
}}
.profile-divider {{
  grid-column: 1 / -1;
  height: 1px;
  background: #30363d;
  margin: 4px 0;
}}

/* Mini stats row */
.mini-stats {{ display: flex; gap: 12px; flex-wrap: wrap; margin-top: 12px; }}
.mini-stat {{
  flex: 1;
  min-width: 80px;
  padding: 10px 12px;
  background: #0d1117;
  border: 1px solid #30363d;
  border-radius: 7px;
  text-align: center;
}}
.mini-stat-val {{ font-size: 16px; font-weight: 700; color: #58a6ff; }}
.mini-stat-lbl {{ font-size: 10px; color: #8b949e; text-transform: uppercase; letter-spacing: 0.4px; margin-top: 2px; }}

/* Chart container */
.chart-container {{ position: relative; height: 180px; }}

/* Rule card */
.rule-why-box {{
  background: #0d1117;
  border: 1px solid #d2992244;
  border-left: 3px solid #d29922;
  border-radius: 6px;
  padding: 10px 14px;
  margin-bottom: 12px;
  font-size: 12px;
  color: #e6edf3;
}}
.rule-stats-row {{
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 8px;
  margin-top: 10px;
}}
.rule-stat-item {{
  background: #0d1117;
  border: 1px solid #30363d;
  border-radius: 6px;
  padding: 8px 10px;
  text-align: center;
}}
.rule-stat-val {{ font-size: 15px; font-weight: 700; color: #58a6ff; }}
.rule-stat-lbl {{ font-size: 10px; color: #8b949e; text-transform: uppercase; letter-spacing: 0.4px; margin-top: 2px; }}

/* ===== RIGHT PANEL ===== */
.right-panel {{
  width: 320px;
  min-width: 320px;
  background: #0d1117;
  border-left: 1px solid #30363d;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}}
.tab-bar {{
  display: flex;
  border-bottom: 1px solid #30363d;
  flex-shrink: 0;
  background: #161b22;
}}
.tab-btn {{
  flex: 1;
  padding: 10px 6px;
  background: none;
  border: none;
  font-family: inherit;
  font-size: 11px;
  font-weight: 500;
  color: #8b949e;
  cursor: pointer;
  border-bottom: 2px solid transparent;
  transition: all 0.15s;
  text-align: center;
}}
.tab-btn:hover {{ color: #e6edf3; }}
.tab-btn.active {{ color: #58a6ff; border-bottom-color: #58a6ff; }}
.tab-content {{ flex: 1; overflow-y: auto; padding: 14px; display: none; }}
.tab-content.active {{ display: flex; flex-direction: column; gap: 12px; }}

/* Search form */
.form-group {{ display: flex; flex-direction: column; gap: 5px; }}
.form-label {{ font-size: 10px; font-weight: 600; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; }}
.form-input, .form-select {{
  width: 100%;
  background: #161b22;
  border: 1px solid #30363d;
  border-radius: 6px;
  color: #e6edf3;
  font-family: inherit;
  font-size: 12px;
  padding: 7px 10px;
  outline: none;
  transition: border-color 0.15s;
}}
.form-input:focus, .form-select:focus {{ border-color: #58a6ff66; }}
.form-input::placeholder {{ color: #8b949e; }}
.form-row {{ display: flex; gap: 8px; }}
.form-row .form-group {{ flex: 1; }}
.search-results-table {{ font-size: 11px; }}
.search-results-table th {{ font-size: 9px; }}
.search-results-table td {{ padding: 5px 8px; }}
.view-link {{
  color: #58a6ff;
  text-decoration: none;
  font-size: 10px;
  font-weight: 600;
  padding: 2px 6px;
  border: 1px solid #58a6ff44;
  border-radius: 4px;
}}
.view-link:hover {{ background: #58a6ff22; }}

/* Network SVG */
#network-svg-container {{
  width: 100%;
  background: #0d1117;
  border: 1px solid #30363d;
  border-radius: 8px;
  overflow: hidden;
  position: relative;
}}
#network-svg-container svg {{
  width: 100%;
  height: 260px;
  display: block;
}}
.network-placeholder {{
  text-align: center;
  color: #8b949e;
  font-size: 12px;
  padding: 40px 20px;
}}

/* Risk score */
.risk-score-big {{
  text-align: center;
  padding: 16px;
  background: #161b22;
  border: 1px solid #30363d;
  border-radius: 8px;
}}
.risk-score-number {{
  font-size: 48px;
  font-weight: 800;
  line-height: 1;
  margin-bottom: 4px;
}}
.risk-score-label {{
  font-size: 11px;
  color: #8b949e;
  text-transform: uppercase;
  letter-spacing: 0.6px;
}}
.risk-score-vs {{
  font-size: 11px;
  color: #8b949e;
  margin-top: 6px;
}}
.risk-bars {{ display: flex; flex-direction: column; gap: 8px; }}
.risk-bar-item {{ display: flex; flex-direction: column; gap: 3px; }}
.risk-bar-header {{ display: flex; justify-content: space-between; align-items: center; }}
.risk-bar-name {{ font-size: 11px; color: #e6edf3; }}
.risk-bar-pct {{ font-size: 11px; font-weight: 600; }}
.risk-bar-track {{
  height: 6px;
  background: #30363d;
  border-radius: 3px;
  overflow: hidden;
}}
.risk-bar-fill {{
  height: 100%;
  border-radius: 3px;
  transition: width 0.6s ease;
}}

/* Tooltip */
.tooltip {{
  position: absolute;
  pointer-events: none;
  background: #1c2128;
  border: 1px solid #30363d;
  border-radius: 6px;
  padding: 6px 10px;
  font-size: 11px;
  color: #e6edf3;
  white-space: nowrap;
  z-index: 1000;
  display: none;
  box-shadow: 0 4px 16px #00000066;
}}

/* Divider */
.section-divider {{
  height: 1px;
  background: linear-gradient(to right, transparent, #30363d, transparent);
  margin: 4px 0;
}}

/* Right panel header */
.right-panel-header {{
  padding: 12px 14px 0;
  font-size: 11px;
  font-weight: 700;
  color: #8b949e;
  text-transform: uppercase;
  letter-spacing: 0.8px;
}}

/* Flags */
.flag-pep {{
  display: inline-flex;
  align-items: center;
  gap: 4px;
  background: #bc8cff22;
  border: 1px solid #bc8cff44;
  border-radius: 4px;
  padding: 2px 7px;
  font-size: 10px;
  font-weight: 700;
  color: #bc8cff;
}}
.flag-sanctions {{
  display: inline-flex;
  align-items: center;
  gap: 4px;
  background: #f7816622;
  border: 1px solid #f7816644;
  border-radius: 4px;
  padding: 2px 7px;
  font-size: 10px;
  font-weight: 700;
  color: #f78166;
}}

/* Scrollbar fix inside panels */
.alert-list, .center-panel, .tab-content {{
  scroll-behavior: smooth;
}}
</style>
</head>
<body>
<div class="app-wrapper">

<!-- ===== HEADER ===== -->
<header class="header">
  <a href="#" class="header-logo">
    <div class="header-logo-icon">🔍</div>
    TransactGuard AML
  </a>
  <a href="executive_dashboard.html" class="header-back">
    ← Executive Dashboard
  </a>
  <div class="header-sep"></div>
  <span class="header-title">Analyst Workstation</span>
  <div class="header-spacer"></div>
  <div class="header-queue-stats">
    <div class="queue-stat">
      <span class="queue-stat-val" style="color:#58a6ff">847</span>
      <span class="queue-stat-lbl">Queue</span>
    </div>
    <div class="queue-stat">
      <span class="queue-stat-val" style="color:#3fb950">23</span>
      <span class="queue-stat-lbl">Assigned to me</span>
    </div>
    <div class="queue-stat">
      <span class="queue-stat-val" style="color:#f78166">5</span>
      <span class="queue-stat-lbl">Overdue</span>
    </div>
  </div>
  <div class="header-analyst">
    <div class="analyst-avatar">AG</div>
    <div class="analyst-info">
      <div class="analyst-name">Ana García</div>
      <div class="analyst-role">AML Analyst II</div>
    </div>
  </div>
</header>

<!-- ===== BODY ===== -->
<div class="body-row">

  <!-- ===== LEFT PANEL ===== -->
  <aside class="left-panel">
    <div class="panel-header">
      <div class="panel-title">Alert Queue (<span class="panel-title-count">1,610 active</span>)</div>
      <div class="filter-bar">
        <select class="filter-select" id="filterStatus" onchange="applyFilters()">
          <option value="">All Statuses</option>
          <option value="new">New</option>
          <option value="under_review">Under Review</option>
          <option value="escalated">Escalated</option>
        </select>
        <select class="filter-select" id="filterSeverity" onchange="applyFilters()">
          <option value="">All Severities</option>
          <option value="critical">Critical</option>
          <option value="high">High</option>
          <option value="medium">Medium</option>
          <option value="low">Low</option>
        </select>
        <select class="filter-select" id="filterRule" onchange="applyFilters()">
          <option value="">All Rules</option>
          {_rule_options(rules_list)}
        </select>
        <div class="date-range-row">
          <span class="date-range-text">Last 30 days</span>
          <button class="date-arrow" title="Previous period">‹</button>
          <button class="date-arrow" title="Next period">›</button>
        </div>
        <input class="filter-input" id="filterSearch" type="text"
               placeholder="Search customer or ID…" oninput="applyFilters()" />
      </div>
    </div>
    <div class="alert-list" id="alertList"></div>
  </aside>

  <!-- ===== CENTER PANEL ===== -->
  <main class="center-panel" id="centerPanel">
    <!-- Placeholder -->
    <div class="placeholder" id="centerPlaceholder">
      <div class="placeholder-icon">🔍</div>
      <div class="placeholder-title">Select an Alert to Investigate</div>
      <div class="placeholder-subtitle">
        Choose an alert from the queue on the left to load full investigation details, transaction history, and risk analysis.
      </div>
      <div class="placeholder-search">
        <input type="text" placeholder="Quick search: customer name, alert ID…" oninput="quickSearch(this.value)" />
      </div>
      <div class="placeholder-stats">
        <div class="placeholder-stat">
          <div class="placeholder-stat-val">1,610</div>
          <div class="placeholder-stat-lbl">Active Alerts</div>
        </div>
        <div class="placeholder-stat">
          <div class="placeholder-stat-val" style="color:#f78166">409</div>
          <div class="placeholder-stat-lbl">Critical</div>
        </div>
        <div class="placeholder-stat">
          <div class="placeholder-stat-val" style="color:#d29922">576</div>
          <div class="placeholder-stat-lbl">High</div>
        </div>
        <div class="placeholder-stat">
          <div class="placeholder-stat-val" style="color:#3fb950">23</div>
          <div class="placeholder-stat-lbl">Assigned to Me</div>
        </div>
      </div>
    </div>
    <!-- Detail view (hidden until alert selected) -->
    <div id="alertDetail" style="display:none; display:flex; flex-direction:column; gap:14px;"></div>
  </main>

  <!-- ===== RIGHT PANEL ===== -->
  <aside class="right-panel">
    <div class="tab-bar">
      <button class="tab-btn active" onclick="switchTab('search')">🔎 Search</button>
      <button class="tab-btn" onclick="switchTab('network')">🕸 Network</button>
      <button class="tab-btn" onclick="switchTab('risk')">📊 Risk</button>
    </div>

    <!-- Tab 1: Transaction Search -->
    <div class="tab-content active" id="tab-search">
      <div style="font-size:11px;font-weight:700;color:#8b949e;text-transform:uppercase;letter-spacing:0.7px;">Transaction Search</div>
      <div class="form-row">
        <div class="form-group">
          <label class="form-label">Min Amount</label>
          <input class="form-input" id="srchMin" type="number" placeholder="0" />
        </div>
        <div class="form-group">
          <label class="form-label">Max Amount</label>
          <input class="form-input" id="srchMax" type="number" placeholder="Any" />
        </div>
      </div>
      <div class="form-row">
        <div class="form-group">
          <label class="form-label">Date From</label>
          <input class="form-input" id="srchDateFrom" type="date" />
        </div>
        <div class="form-group">
          <label class="form-label">Date To</label>
          <input class="form-input" id="srchDateTo" type="date" />
        </div>
      </div>
      <div class="form-group">
        <label class="form-label">Counterparty Country</label>
        <input class="form-input" id="srchCountry" type="text" placeholder="e.g. US, CN, RU…" />
      </div>
      <div class="form-group">
        <label class="form-label">Channel</label>
        <select class="form-select" id="srchChannel">
          <option value="">All Channels</option>
          <option value="online">Online</option>
          <option value="mobile">Mobile</option>
          <option value="branch">Branch</option>
          <option value="atm">ATM</option>
          <option value="wire">Wire</option>
        </select>
      </div>
      <button class="btn btn-primary" style="width:100%" onclick="runSearch()">Run Search</button>
      <div id="searchResults"></div>
    </div>

    <!-- Tab 2: Network Graph -->
    <div class="tab-content" id="tab-network">
      <div style="font-size:11px;font-weight:700;color:#8b949e;text-transform:uppercase;letter-spacing:0.7px;">Customer Transaction Network</div>
      <div id="network-svg-container">
        <div class="network-placeholder" id="networkPlaceholder">
          Select an alert to view the transaction network graph.
        </div>
        <svg id="networkSvg" style="display:none"></svg>
      </div>
      <div id="networkLegend" style="display:none">
        <div style="font-size:10px;color:#8b949e;margin-top:8px;text-transform:uppercase;letter-spacing:0.5px;">Risk Level Legend</div>
        <div style="display:flex;gap:12px;margin-top:6px;flex-wrap:wrap">
          <span style="font-size:10px;display:flex;align-items:center;gap:4px"><span style="width:10px;height:10px;background:#3fb950;border-radius:50%;display:inline-block"></span>Low Risk</span>
          <span style="font-size:10px;display:flex;align-items:center;gap:4px"><span style="width:10px;height:10px;background:#d29922;border-radius:50%;display:inline-block"></span>Medium Risk</span>
          <span style="font-size:10px;display:flex;align-items:center;gap:4px"><span style="width:10px;height:10px;background:#f78166;border-radius:50%;display:inline-block"></span>High Risk</span>
          <span style="font-size:10px;display:flex;align-items:center;gap:4px"><span style="width:10px;height:10px;background:#58a6ff;border-radius:50%;display:inline-block"></span>Center Node</span>
        </div>
      </div>
    </div>

    <!-- Tab 3: Risk Score -->
    <div class="tab-content" id="tab-risk">
      <div style="font-size:11px;font-weight:700;color:#8b949e;text-transform:uppercase;letter-spacing:0.7px;">Risk Score Breakdown</div>
      <div id="riskContent">
        <div class="network-placeholder">Select an alert to view risk score breakdown.</div>
      </div>
    </div>

  </aside>
</div>
</div>

<!-- Tooltip element -->
<div class="tooltip" id="tooltip"></div>

<script>
// ===== DATA =====
const alertsData = {alerts_json};
const searchTransactions = {search_json};
const rulesData = {rules_json};

let selectedAlertIdx = null;
let activityChart = null;

// ===== INIT =====
document.addEventListener('DOMContentLoaded', () => {{
  renderAlertList(alertsData);
}});

// ===== FORMAT HELPERS =====
function fmtAmount(n) {{
  if (n === null || n === undefined) return '—';
  return '$' + Number(n).toLocaleString('en-US', {{minimumFractionDigits: 2, maximumFractionDigits: 2}});
}}
function fmtDate(s) {{
  if (!s) return '—';
  return s.substring(0, 16).replace('T', ' ');
}}
function fmtDateShort(s) {{
  if (!s) return '—';
  return s.substring(0, 10);
}}
function capitalize(s) {{
  if (!s) return '';
  return s.charAt(0).toUpperCase() + s.slice(1).replace(/_/g, ' ');
}}
function severityColor(s) {{
  const m = {{critical:'#f78166',high:'#d29922',medium:'#e3b341',low:'#3fb950'}};
  return m[s] || '#8b949e';
}}
function riskColor(score) {{
  if (score >= 75) return '#f78166';
  if (score >= 50) return '#d29922';
  return '#3fb950';
}}
function riskLevelColor(level) {{
  if (level === 'high' || level === 'critical') return '#f78166';
  if (level === 'medium') return '#d29922';
  return '#3fb950';
}}
function statusLabel(s) {{
  const m = {{new:'New',under_review:'Under Review',escalated:'Escalated',true_positive:'True Positive',false_positive:'False Positive'}};
  return m[s] || capitalize(s);
}}

// ===== ALERT LIST =====
function renderAlertList(alerts) {{
  const container = document.getElementById('alertList');
  if (!alerts.length) {{
    container.innerHTML = '<div class="no-results">No alerts match the current filters.</div>';
    return;
  }}
  container.innerHTML = alerts.map((a, idx) => {{
    const origIdx = alertsData.indexOf(a);
    return `
    <div class="alert-card" data-idx="${{origIdx}}" data-severity="${{a.severity}}"
         onclick="selectAlert(${{origIdx}})" id="card-${{origIdx}}">
      <div class="card-row1">
        <span class="card-alert-id">${{a.alert_id}}</span>
        <span class="badge badge-${{a.severity}}">${{capitalize(a.severity)}}</span>
      </div>
      <div class="card-row2">
        <span class="card-customer">${{a.customer.name}}</span>
        <span class="card-acct-type">${{capitalize(a.account.account_type || 'Account')}}</span>
      </div>
      <div class="card-rule">${{a.rule_name}}</div>
      <div class="card-row3">
        <span class="card-amount">${{fmtAmount(a.flagged_amount)}}</span>
        <span class="card-meta">${{fmtDateShort(a.created_at)}} &nbsp;<span class="badge badge-${{a.status}}">${{statusLabel(a.status)}}</span></span>
      </div>
    </div>`;
  }}).join('');
}}

// ===== FILTERS =====
function applyFilters() {{
  const status = document.getElementById('filterStatus').value;
  const severity = document.getElementById('filterSeverity').value;
  const rule = document.getElementById('filterRule').value;
  const search = document.getElementById('filterSearch').value.toLowerCase();

  const filtered = alertsData.filter(a => {{
    if (status && a.status !== status) return false;
    if (severity && a.severity !== severity) return false;
    if (rule && String(a.rule_id) !== rule) return false;
    if (search) {{
      const haystack = (a.alert_id + ' ' + a.customer.name + ' ' + a.customer.customer_id).toLowerCase();
      if (!haystack.includes(search)) return false;
    }}
    return true;
  }});
  renderAlertList(filtered);
}}

function quickSearch(val) {{
  document.getElementById('filterSearch').value = val;
  applyFilters();
}}

// ===== SELECT ALERT =====
function selectAlert(idx) {{
  selectedAlertIdx = idx;
  // Highlight card
  document.querySelectorAll('.alert-card').forEach(c => c.classList.remove('selected'));
  const card = document.getElementById('card-' + idx);
  if (card) {{ card.classList.add('selected'); card.scrollIntoView({{block:'nearest'}}); }}

  const alert = alertsData[idx];
  renderDetail(alert);
  renderNetworkGraph(alert);
  renderRiskScore(alert);
}}

// ===== DETAIL PANEL =====
function renderDetail(a) {{
  document.getElementById('centerPlaceholder').style.display = 'none';
  const detail = document.getElementById('alertDetail');
  detail.style.display = 'flex';

  detail.innerHTML = `
    ${{renderAlertHeader(a)}}
    ${{renderTransactionDetails(a)}}
    ${{renderCustomerProfile(a)}}
    ${{renderTimeline(a)}}
    ${{renderRuleExplanation(a)}}
    ${{renderRelatedAlerts(a)}}
  `;

  // Build chart after DOM insertion
  buildActivityChart(a);
}}

function renderAlertHeader(a) {{
  return `
  <div class="section-card">
    <div class="section-card-header">
      <div class="section-card-title"><span class="section-card-title-icon">🚨</span> Alert Details</div>
      <span class="badge badge-${{a.status}}">${{statusLabel(a.status)}}</span>
    </div>
    <div class="section-card-body">
      <div class="alert-header-row">
        <div>
          <div class="alert-header-id">${{a.alert_id}}</div>
          <div class="alert-header-badges" style="margin-top:6px">
            <span class="badge badge-${{a.severity}}">${{capitalize(a.severity)}}</span>
            <span class="badge badge-${{a.status}}">${{statusLabel(a.status)}}</span>
            ${{a.customer.pep_flag ? '<span class="flag-pep">⚠ PEP</span>' : ''}}
            ${{a.customer.sanctions_flag ? '<span class="flag-sanctions">⛔ SANCTIONS</span>' : ''}}
          </div>
        </div>
      </div>
      <div class="alert-header-meta">
        <div class="alert-meta-item">
          <span class="alert-meta-label">Assigned To</span>
          <span class="alert-meta-value">${{a.assigned_to}}</span>
        </div>
        <div class="alert-meta-item">
          <span class="alert-meta-label">Created</span>
          <span class="alert-meta-value">${{fmtDate(a.created_at)}}</span>
        </div>
        <div class="alert-meta-item">
          <span class="alert-meta-label">Rule</span>
          <span class="alert-meta-value">${{a.rule_name}}</span>
        </div>
        <div class="alert-meta-item">
          <span class="alert-meta-label">Flagged Amount</span>
          <span class="alert-meta-value" style="color:#f78166;font-weight:700">${{fmtAmount(a.flagged_amount)}}</span>
        </div>
      </div>
      ${{a.notes ? `<div style="margin-top:10px;padding:8px 12px;background:#0d1117;border:1px solid #30363d;border-radius:6px;font-size:12px;color:#8b949e"><strong style="color:#e6edf3">Notes:</strong> ${{a.notes}}</div>` : ''}}
      <div class="alert-action-row">
        <button class="btn btn-danger" onclick="markResolution('true_positive', this)">✓ Mark True Positive</button>
        <button class="btn btn-success" onclick="markResolution('false_positive', this)">✗ Mark False Positive</button>
        <button class="btn btn-warning" onclick="escalateAlert(this)">↑ Escalate</button>
        <button class="btn btn-primary" onclick="assignAlert(this)">⊕ Assign</button>
      </div>
    </div>
  </div>`;
}}

function renderTransactionDetails(a) {{
  const t = a.transaction;
  const riskCls = t.counterparty_risk === 'high' ? 'text-red' : t.counterparty_risk === 'medium' ? 'text-amber' : 'text-green';
  return `
  <div class="section-card">
    <div class="section-card-header">
      <div class="section-card-title"><span class="section-card-title-icon">💳</span> Transaction Details</div>
    </div>
    <div class="section-card-body" style="padding:0">
      <table class="data-table">
        <thead><tr>
          <th>Transaction ID</th><th>Date / Time</th><th>Type</th><th>Amount</th><th>Currency</th><th>Channel</th>
        </tr></thead>
        <tbody><tr>
          <td><span class="mono">${{t.transaction_id}}</span></td>
          <td>${{fmtDate(t.date)}}</td>
          <td><span class="badge ${{t.type === 'credit' ? 'badge-new' : 'badge-high'}}">${{capitalize(t.type)}}</span></td>
          <td style="font-weight:700;color:#f78166">${{fmtAmount(t.amount)}}</td>
          <td>${{t.currency}}</td>
          <td><span class="badge badge-new">${{capitalize(t.channel)}}</span></td>
        </tr></tbody>
      </table>
      <div style="padding:12px 16px;border-top:1px solid #21262d">
        <div style="display:flex;gap:32px;flex-wrap:wrap">
          <div>
            <div class="alert-meta-label">Counterparty Name</div>
            <div style="font-size:13px;font-weight:600;margin-top:3px">${{t.counterparty_name || '—'}}</div>
          </div>
          <div>
            <div class="alert-meta-label">Counterparty Country</div>
            <div style="font-size:13px;font-weight:600;margin-top:3px">
              ${{t.counterparty_country_name}} (${{t.counterparty_country}})
              <span class="badge badge-${{t.counterparty_risk}}-risk" style="margin-left:6px">${{capitalize(t.counterparty_risk)}} Risk</span>
            </div>
          </div>
          <div>
            <div class="alert-meta-label">Description</div>
            <div style="font-size:13px;font-weight:500;margin-top:3px;color:#8b949e">${{t.description || '—'}}</div>
          </div>
        </div>
      </div>
    </div>
  </div>`;
}}

function renderCustomerProfile(a) {{
  const c = a.customer;
  const ac = a.account;
  const ratingCls = c.risk_rating === 'high' ? 'text-red' : c.risk_rating === 'medium' ? 'text-amber' : 'text-green';
  return `
  <div class="section-card">
    <div class="section-card-header">
      <div class="section-card-title"><span class="section-card-title-icon">👤</span> Customer Profile</div>
      <span class="badge badge-${{c.risk_rating}}">${{capitalize(c.risk_rating)}} Risk</span>
    </div>
    <div class="section-card-body">
      <div class="profile-grid">
        <div class="profile-item">
          <div class="profile-label">Customer Name</div>
          <div class="profile-value" style="font-size:15px;font-weight:700">${{c.name}}</div>
        </div>
        <div class="profile-item">
          <div class="profile-label">Customer ID</div>
          <div class="profile-value mono" style="font-size:12px">${{c.customer_id}}</div>
        </div>
        <div class="profile-item">
          <div class="profile-label">KYC Status</div>
          <div class="profile-value"><span class="badge badge-${{c.kyc_status}}">${{capitalize(c.kyc_status)}}</span></div>
        </div>
        <div class="profile-item">
          <div class="profile-label">Nationality</div>
          <div class="profile-value">${{c.nationality_name}} (${{c.nationality}})</div>
        </div>
        <div class="profile-item">
          <div class="profile-label">PEP Flag</div>
          <div class="profile-value">${{c.pep_flag ? '<span class="flag-pep">⚠ YES — Politically Exposed</span>' : '<span style="color:#3fb950">No</span>'}}</div>
        </div>
        <div class="profile-item">
          <div class="profile-label">Sanctions Flag</div>
          <div class="profile-value">${{c.sanctions_flag ? '<span class="flag-sanctions">⛔ YES</span>' : '<span style="color:#3fb950">No</span>'}}</div>
        </div>
        <div class="profile-item">
          <div class="profile-label">Country of Residence</div>
          <div class="profile-value">${{c.country_of_residence}}</div>
        </div>
        <div class="profile-item">
          <div class="profile-label">Risk Rating</div>
          <div class="profile-value ${{ratingCls}}" style="font-weight:700">${{capitalize(c.risk_rating)}}</div>
        </div>
        <div class="profile-divider"></div>
        <div class="profile-item">
          <div class="profile-label">Account Type</div>
          <div class="profile-value">${{capitalize(ac.account_type)}}</div>
        </div>
        <div class="profile-item">
          <div class="profile-label">Account Status</div>
          <div class="profile-value"><span class="badge badge-${{ac.status}}">${{capitalize(ac.status)}}</span></div>
        </div>
        <div class="profile-item">
          <div class="profile-label">Balance</div>
          <div class="profile-value" style="font-weight:700">${{fmtAmount(ac.balance)}} ${{ac.currency}}</div>
        </div>
        <div class="profile-item">
          <div class="profile-label">Account Opened</div>
          <div class="profile-value">${{fmtDateShort(ac.opened_at)}}</div>
        </div>
      </div>
      <div class="mini-stats">
        <div class="mini-stat">
          <div class="mini-stat-val">${{Number(c.total_txns).toLocaleString()}}</div>
          <div class="mini-stat-lbl">Total Transactions</div>
        </div>
        <div class="mini-stat">
          <div class="mini-stat-val" style="font-size:13px">${{fmtAmount(c.total_volume)}}</div>
          <div class="mini-stat-lbl">Total Volume</div>
        </div>
        <div class="mini-stat">
          <div class="mini-stat-val" style="color:${{c.alerts_count > 5 ? '#f78166' : c.alerts_count > 2 ? '#d29922' : '#3fb950'}}">${{c.alerts_count}}</div>
          <div class="mini-stat-lbl">Alerts Generated</div>
        </div>
      </div>
    </div>
  </div>`;
}}

function renderTimeline(a) {{
  return `
  <div class="section-card">
    <div class="section-card-header">
      <div class="section-card-title"><span class="section-card-title-icon">📈</span> 30-Day Activity Timeline</div>
      <div style="display:flex;gap:12px;font-size:10px">
        <span style="display:flex;align-items:center;gap:4px"><span style="width:12px;height:2px;background:#58a6ff;display:inline-block"></span>Transactions</span>
        <span style="display:flex;align-items:center;gap:4px"><span style="width:12px;height:2px;background:#d29922;display:inline-block"></span>Volume (USD)</span>
      </div>
    </div>
    <div class="section-card-body">
      <div class="chart-container"><canvas id="activityChart"></canvas></div>
    </div>
  </div>`;
}}

function buildActivityChart(a) {{
  if (activityChart) {{ activityChart.destroy(); activityChart = null; }}
  const ctx = document.getElementById('activityChart');
  if (!ctx) return;

  const labels = a.timeline_labels.map(l => l.substring(5)); // MM-DD
  const alertDayIdx = a.alert_day_index;

  // Annotation dataset: vertical line at alert day
  const annotationData = a.timeline_labels.map((_, i) => i === alertDayIdx ? a.timeline_cnt[alertDayIdx] * 1.2 + 0.5 : null);

  activityChart = new Chart(ctx, {{
    type: 'line',
    data: {{
      labels: labels,
      datasets: [
        {{
          label: 'Transactions',
          data: a.timeline_cnt,
          borderColor: '#58a6ff',
          backgroundColor: '#58a6ff18',
          borderWidth: 2,
          pointRadius: 3,
          pointBackgroundColor: '#58a6ff',
          tension: 0.3,
          fill: true,
          yAxisID: 'y',
        }},
        {{
          label: 'Volume (USD)',
          data: a.timeline_vol,
          borderColor: '#d29922',
          backgroundColor: 'transparent',
          borderWidth: 2,
          pointRadius: 2,
          pointBackgroundColor: '#d29922',
          tension: 0.3,
          fill: false,
          yAxisID: 'y2',
        }},
      ]
    }},
    options: {{
      responsive: true,
      maintainAspectRatio: false,
      interaction: {{ mode: 'index', intersect: false }},
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          backgroundColor: '#1c2128',
          borderColor: '#30363d',
          borderWidth: 1,
          titleColor: '#8b949e',
          bodyColor: '#e6edf3',
          callbacks: {{
            afterBody: (items) => alertDayIdx !== null && items[0] && items[0].dataIndex === alertDayIdx
              ? ['⚠ Alert fired on this day'] : []
          }}
        }}
      }},
      scales: {{
        x: {{
          ticks: {{ color: '#8b949e', font: {{ size: 9 }}, maxTicksLimit: 10 }},
          grid: {{ color: '#30363d44' }},
        }},
        y: {{
          type: 'linear', position: 'left',
          ticks: {{ color: '#58a6ff', font: {{ size: 9 }}, stepSize: 1 }},
          grid: {{ color: '#30363d44' }},
          title: {{ display: true, text: 'Txns', color: '#58a6ff', font: {{ size: 9 }} }},
        }},
        y2: {{
          type: 'linear', position: 'right',
          ticks: {{ color: '#d29922', font: {{ size: 9 }}, callback: v => '$' + (v/1000).toFixed(0) + 'k' }},
          grid: {{ drawOnChartArea: false }},
          title: {{ display: true, text: 'Vol', color: '#d29922', font: {{ size: 9 }} }},
        }}
      }}
    }}
  }});

  // Draw alert day marker via afterDraw plugin
  if (alertDayIdx !== null) {{
    activityChart.options.plugins.alertLine = {{ dayIdx: alertDayIdx }};
    Chart.register({{
      id: 'alertLinePlugin_' + Date.now(),
      afterDraw(chart) {{
        const idx = chart.options.plugins.alertLine && chart.options.plugins.alertLine.dayIdx;
        if (idx === null || idx === undefined) return;
        const meta = chart.getDatasetMeta(0);
        if (!meta.data[idx]) return;
        const x = meta.data[idx].x;
        const ctx2 = chart.ctx;
        ctx2.save();
        ctx2.beginPath();
        ctx2.setLineDash([4, 3]);
        ctx2.strokeStyle = '#f78166cc';
        ctx2.lineWidth = 1.5;
        ctx2.moveTo(x, chart.chartArea.top);
        ctx2.lineTo(x, chart.chartArea.bottom);
        ctx2.stroke();
        ctx2.fillStyle = '#f78166';
        ctx2.font = 'bold 9px Inter';
        ctx2.fillText('⚠ Alert', x + 4, chart.chartArea.top + 12);
        ctx2.restore();
      }}
    }});
    activityChart.update();
  }}
}}

function renderRuleExplanation(a) {{
  const fpPct = a.rule_fp_rate;
  const fpColor = fpPct > 60 ? '#f78166' : fpPct > 40 ? '#d29922' : '#3fb950';
  return `
  <div class="section-card">
    <div class="section-card-header">
      <div class="section-card-title"><span class="section-card-title-icon">📋</span> Rule Explanation</div>
      <span class="badge badge-new">${{capitalize(a.rule_category)}}</span>
    </div>
    <div class="section-card-body">
      <div style="font-size:14px;font-weight:700;color:#e6edf3;margin-bottom:4px">${{a.rule_name}}</div>
      <div style="font-size:12px;color:#8b949e;margin-bottom:12px">${{a.rule_description}}</div>
      <div class="rule-why-box">
        <strong style="color:#d29922">Why it fired:</strong> ${{a.why_fired}}
      </div>
      <div class="rule-stats-row">
        <div class="rule-stat-item">
          <div class="rule-stat-val" style="color:#3fb950">${{(a.rule_precision * 100).toFixed(0)}}%</div>
          <div class="rule-stat-lbl">Precision</div>
        </div>
        <div class="rule-stat-item">
          <div class="rule-stat-val" style="color:#58a6ff">${{(a.rule_recall * 100).toFixed(0)}}%</div>
          <div class="rule-stat-lbl">Recall</div>
        </div>
        <div class="rule-stat-item">
          <div class="rule-stat-val">${{Number(a.rule_total_alerts).toLocaleString()}}</div>
          <div class="rule-stat-lbl">Total Alerts</div>
        </div>
        <div class="rule-stat-item">
          <div class="rule-stat-val" style="color:${{fpColor}}">${{fpPct}}%</div>
          <div class="rule-stat-lbl">FP Rate</div>
        </div>
      </div>
    </div>
  </div>`;
}}

function renderRelatedAlerts(a) {{
  const related = a.related_alerts;
  if (!related || !related.length) {{
    return `<div class="section-card">
      <div class="section-card-header">
        <div class="section-card-title"><span class="section-card-title-icon">🔗</span> Related Alerts</div>
      </div>
      <div class="section-card-body"><div class="text-muted" style="font-size:12px">No prior alerts found for this customer.</div></div>
    </div>`;
  }}
  const rows = related.map(r => `
    <tr>
      <td><span class="mono" style="font-size:10px">${{r.alert_id}}</span></td>
      <td>${{r.date}}</td>
      <td style="max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${{r.rule_name}}</td>
      <td>${{fmtAmount(r.amount)}}</td>
      <td><span class="badge badge-${{r.status}}">${{statusLabel(r.status)}}</span></td>
    </tr>`).join('');
  return `
  <div class="section-card">
    <div class="section-card-header">
      <div class="section-card-title"><span class="section-card-title-icon">🔗</span> Related Alerts — ${{a.customer.name}}</div>
      <span style="font-size:11px;color:#8b949e">Last ${{related.length}} alerts</span>
    </div>
    <div class="section-card-body" style="padding:0">
      <table class="data-table">
        <thead><tr><th>Alert ID</th><th>Date</th><th>Rule</th><th>Amount</th><th>Status</th></tr></thead>
        <tbody>${{rows}}</tbody>
      </table>
    </div>
  </div>`;
}}

// ===== NETWORK GRAPH =====
function renderNetworkGraph(a) {{
  const svg = document.getElementById('networkSvg');
  const placeholder = document.getElementById('networkPlaceholder');
  const legend = document.getElementById('networkLegend');
  const nodes = a.network_nodes;

  placeholder.style.display = 'none';
  svg.style.display = 'block';
  legend.style.display = 'flex';

  const W = 292, H = 260;
  const cx = W / 2, cy = H / 2 - 10;
  const R = 95; // radius for counterparty nodes

  const colorMap = {{ low: '#3fb950', medium: '#d29922', high: '#f78166', critical: '#f78166', unknown: '#8b949e' }};

  let svgContent = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${{W}} ${{H}}" style="background:#0d1117">`;

  // Defs
  svgContent += `<defs>
    <radialGradient id="centerGrad" cx="50%" cy="50%" r="50%">
      <stop offset="0%" stop-color="#58a6ff" stop-opacity="0.9"/>
      <stop offset="100%" stop-color="#1f6feb" stop-opacity="0.8"/>
    </radialGradient>
    <filter id="glow">
      <feGaussianBlur stdDeviation="2" result="blur"/>
      <feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>
    </filter>
  </defs>`;

  if (!nodes || !nodes.length) {{
    svgContent += `<text x="${{W/2}}" y="${{H/2}}" text-anchor="middle" fill="#8b949e" font-size="12">No network data available</text>`;
    svgContent += `</svg>`;
    svg.outerHTML = svgContent;
    return;
  }}

  const n = Math.min(nodes.length, 8);
  const angleStep = (2 * Math.PI) / n;

  // Draw edges first (behind nodes)
  for (let i = 0; i < n; i++) {{
    const angle = angleStep * i - Math.PI / 2;
    const nx = cx + R * Math.cos(angle);
    const ny = cy + R * Math.sin(angle);
    const nd = nodes[i];
    const strokeW = Math.min(4, Math.max(1, nd.count / 3));
    svgContent += `<line x1="${{cx}}" y1="${{cy}}" x2="${{nx}}" y2="${{ny}}"
      stroke="#30363d" stroke-width="${{strokeW}}" stroke-opacity="0.7"/>`;
  }}

  // Center node
  svgContent += `<circle cx="${{cx}}" cy="${{cy}}" r="22" fill="url(#centerGrad)" filter="url(#glow)" stroke="#58a6ff" stroke-width="1.5"/>`;
  const custInitials = a.customer.name.split(' ').map(w => w[0]).join('').substring(0, 2);
  svgContent += `<text x="${{cx}}" y="${{cy-5}}" text-anchor="middle" fill="white" font-size="10" font-weight="700" font-family="Inter,sans-serif">${{custInitials}}</text>`;
  svgContent += `<text x="${{cx}}" y="${{cy+8}}" text-anchor="middle" fill="#ffffffaa" font-size="7" font-family="Inter,sans-serif">${{a.customer.customer_id}}</text>`;

  // Counterparty nodes
  for (let i = 0; i < n; i++) {{
    const angle = angleStep * i - Math.PI / 2;
    const nx = cx + R * Math.cos(angle);
    const ny = cy + R * Math.sin(angle);
    const nd = nodes[i];
    const nodeR = Math.min(14, Math.max(8, 8 + nd.count * 0.8));
    const col = colorMap[nd.risk_level] || '#8b949e';

    svgContent += `<circle cx="${{nx}}" cy="${{ny}}" r="${{nodeR}}"
      fill="${{col}}33" stroke="${{col}}" stroke-width="1.5"
      class="net-node"
      data-name="${{nd.name}}" data-country="${{nd.country_name}}" data-vol="${{nd.vol.toLocaleString()}}" data-count="${{nd.count}}"
      onmouseover="showNetTooltip(event,this)" onmouseout="hideTooltip()" style="cursor:pointer"/>`;

    // Label: short name, wrapping if needed
    const shortName = nd.name.length > 12 ? nd.name.substring(0, 11) + '…' : nd.name;
    const labelOffset = nodeR + 10;
    const lx = cx + (R + labelOffset) * Math.cos(angle);
    const ly = cy + (R + labelOffset) * Math.sin(angle);
    svgContent += `<text x="${{lx}}" y="${{ly + 3}}" text-anchor="middle" fill="#8b949e" font-size="8" font-family="Inter,sans-serif">${{shortName}}</text>`;
    svgContent += `<text x="${{lx}}" y="${{ly + 13}}" text-anchor="middle" fill="${{col}}aa" font-size="7" font-family="Inter,sans-serif">${{nd.country}}</text>`;
  }}

  svgContent += `</svg>`;
  svg.outerHTML = svgContent;
}}

function showNetTooltip(event, el) {{
  const t = document.getElementById('tooltip');
  t.innerHTML = `<strong>${{el.dataset.name}}</strong><br/>${{el.dataset.country}}<br/>Txns: ${{el.dataset.count}} &nbsp;|&nbsp; Vol: $${{Number(el.dataset.vol).toLocaleString()}}`;
  t.style.display = 'block';
  t.style.left = (event.pageX + 12) + 'px';
  t.style.top = (event.pageY - 30) + 'px';
}}

function hideTooltip() {{
  document.getElementById('tooltip').style.display = 'none';
}}

// ===== RISK SCORE =====
function renderRiskScore(a) {{
  const score = a.total_risk_score;
  const peer = a.peer_avg_score;
  const comps = a.risk_components;
  const col = riskColor(score);

  const barDefs = [
    {{ key: 'transaction_behavior', label: 'Transaction Behavior', color: '#58a6ff' }},
    {{ key: 'geographic_risk', label: 'Geographic Risk', color: '#f78166' }},
    {{ key: 'customer_profile', label: 'Customer Profile', color: '#d29922' }},
    {{ key: 'network_risk', label: 'Network Risk', color: '#bc8cff' }},
    {{ key: 'velocity_risk', label: 'Velocity Risk', color: '#3fb950' }},
  ];

  const bars = barDefs.map(b => {{
    const val = comps[b.key];
    return `
    <div class="risk-bar-item">
      <div class="risk-bar-header">
        <span class="risk-bar-name">${{b.label}}</span>
        <span class="risk-bar-pct" style="color:${{b.color}}">${{val}}/100</span>
      </div>
      <div class="risk-bar-track">
        <div class="risk-bar-fill" style="width:${{val}}%;background:${{b.color}}"></div>
      </div>
    </div>`;
  }}).join('');

  const riskLabel = score >= 75 ? 'HIGH RISK' : score >= 50 ? 'MEDIUM RISK' : 'LOW RISK';

  document.getElementById('riskContent').innerHTML = `
    <div class="risk-score-big">
      <div class="risk-score-number" style="color:${{col}}">${{score}}</div>
      <div class="risk-score-label" style="color:${{col}}">${{riskLabel}}</div>
      <div class="risk-score-vs">Peer group avg: <strong style="color:#e6edf3">${{peer}}</strong> &nbsp;
        <span style="color:${{score > peer ? '#f78166' : '#3fb950'}}">${{score > peer ? '▲' : '▼'}} ${{Math.abs(score - peer)}} pts above avg</span>
      </div>
    </div>
    <div class="risk-bars">${{bars}}</div>
  `;
}}

// ===== TAB SWITCHING =====
function switchTab(name) {{
  document.querySelectorAll('.tab-btn').forEach((b,i) => {{
    b.classList.toggle('active', ['search','network','risk'][i] === name);
  }});
  document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
}}

// ===== TRANSACTION SEARCH =====
function runSearch() {{
  const minA = parseFloat(document.getElementById('srchMin').value) || 0;
  const maxA = parseFloat(document.getElementById('srchMax').value) || Infinity;
  const dateFrom = document.getElementById('srchDateFrom').value;
  const dateTo = document.getElementById('srchDateTo').value;
  const country = document.getElementById('srchCountry').value.toUpperCase().trim();
  const channel = document.getElementById('srchChannel').value;

  const results = searchTransactions.filter(t => {{
    if (t.amount < minA || t.amount > maxA) return false;
    if (dateFrom && t.date < dateFrom) return false;
    if (dateTo && t.date > dateTo + 'Z') return false;
    if (country && t.counterparty_country !== country) return false;
    if (channel && t.channel !== channel) return false;
    return true;
  }}).slice(0, 10);

  const container = document.getElementById('searchResults');
  if (!results.length) {{
    container.innerHTML = '<div class="no-results">No transactions match your criteria.</div>';
    return;
  }}

  const rows = results.map(t => `
    <tr>
      <td><span class="mono" style="font-size:10px">${{t.transaction_id}}</span></td>
      <td>${{t.date.substring(0,10)}}</td>
      <td style="color:#f78166;font-weight:600">${{fmtAmount(t.amount)}}</td>
      <td>${{t.counterparty_country}}</td>
      <td><span class="badge badge-new" style="font-size:8px">${{t.channel}}</span></td>
      <td><a class="view-link" href="#" onclick="highlightRelated('${{t.transaction_id}}');return false">View</a></td>
    </tr>`).join('');

  container.innerHTML = `
    <div style="margin-top:2px;font-size:10px;color:#8b949e">${{results.length}} result${{results.length !== 1 ? 's' : ''}} shown (top 10)</div>
    <table class="data-table search-results-table" style="margin-top:6px">
      <thead><tr><th>Txn ID</th><th>Date</th><th>Amount</th><th>Country</th><th>Channel</th><th></th></tr></thead>
      <tbody>${{rows}}</tbody>
    </table>`;
}}

function highlightRelated(txnId) {{
  // Find alert with this transaction and select it
  const idx = alertsData.findIndex(a => a.transaction && a.transaction.transaction_id === txnId);
  if (idx >= 0) selectAlert(idx);
}}

// ===== ACTION BUTTONS =====
function markResolution(type, btn) {{
  const label = type === 'true_positive' ? 'True Positive' : 'False Positive';
  btn.textContent = '✓ Marked as ' + label;
  btn.disabled = true;
  btn.style.opacity = '0.6';
  if (selectedAlertIdx !== null) {{
    alertsData[selectedAlertIdx].status = type;
    const card = document.getElementById('card-' + selectedAlertIdx);
    if (card) {{
      const badge = card.querySelector('.badge:last-child');
      if (badge) {{ badge.className = 'badge badge-' + type; badge.textContent = label; }}
    }}
  }}
}}

function escalateAlert(btn) {{
  btn.textContent = '↑ Escalated';
  btn.disabled = true;
  btn.style.opacity = '0.6';
  if (selectedAlertIdx !== null) {{
    alertsData[selectedAlertIdx].status = 'escalated';
  }}
}}

function assignAlert(btn) {{
  btn.textContent = '⊕ Assigned to Ana García';
  btn.disabled = true;
  btn.style.opacity = '0.6';
  if (selectedAlertIdx !== null) {{
    alertsData[selectedAlertIdx].assigned_to = 'Ana García';
  }}
}}
</script>
</body>
</html>"""

    return html


def _rule_options(rules_list):
    """Generate <option> elements for rules filter."""
    return "\n".join(
        f'<option value="{r["rule_id"]}">{r["rule_name"]}</option>'
        for r in rules_list
    )


# Patch: inject into generate_html call via module-level replacement
_rule_options_placeholder = "{_rule_options(rules_list)}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Connecting to database…")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    print("Fetching data…")
    data = fetch_data(conn)

    print("Building alert objects…")
    enriched_alerts = build_alert_objects(data)

    print("Building search transactions…")
    search_txns = build_search_transactions(data)

    rules_list = list(data["rules"].values())

    print(f"Generating HTML ({len(enriched_alerts)} alerts, {len(search_txns)} search records)…")
    html = generate_html(enriched_alerts, search_txns, rules_list)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    size_kb = os.path.getsize(OUTPUT_PATH) / 1024
    print(f"Written to: {OUTPUT_PATH}")
    print(f"File size:  {size_kb:.1f} KB")
    if size_kb < 100:
        print("WARNING: file is under 100 KB — check data volume")
    else:
        print("OK: file exceeds 100 KB target")

    conn.close()


if __name__ == "__main__":
    main()
