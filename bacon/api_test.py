"""
api_test.py — Automated tests for the Transaction Monitoring API.

Tests all 11 endpoints, validates response shapes, and checks
that filters/pagination work correctly.
"""

from __future__ import annotations

import sys
import json
import requests

BASE = "http://127.0.0.1:5000"
PASS = 0
FAIL = 0


def check(name: str, condition: bool, detail: str = "") -> None:
    global PASS, FAIL
    status = "PASS" if condition else "FAIL"
    if condition:
        PASS += 1
        print(f"  [{status}] {name}")
    else:
        FAIL += 1
        print(f"  [{status}] {name}  --> {detail}")


def get(path: str, params: dict | None = None):
    r = requests.get(BASE + path, params=params, timeout=10)
    return r


print("=" * 60)
print("API Test Suite")
print("=" * 60)

# ── /api/stats ───────────────────────────────────────────────────────────────
print("\n/api/stats")
r = get("/api/stats")
check("status 200", r.status_code == 200)
d = r.json()
check("has customers",    "customers" in d and d["customers"] > 0)
check("has transactions", "transactions" in d and d["transactions"] > 100_000)
check("has alerts",       "alerts" in d and d["alerts"] > 0)
check("has total_volume", "total_volume_usd" in d and d["total_volume_usd"] > 0)
check("alert_rate_pct > 0", d.get("alert_rate_pct", 0) > 0)
print(f"    customers={d['customers']}, transactions={d['transactions']}, "
      f"alerts={d['alerts']}, volume=${d['total_volume_usd']:,.0f}")

# ── /api/transactions ────────────────────────────────────────────────────────
print("\n/api/transactions (default page)")
r = get("/api/transactions")
check("status 200", r.status_code == 200)
d = r.json()
check("has data list",  isinstance(d.get("data"), list))
check("default per_page 50", len(d["data"]) == 50)
check("has total",      d.get("total", 0) > 100_000)
check("has pages",      d.get("pages", 0) > 1)
first_txn = d["data"][0]
check("txn has id",          "id" in first_txn)
check("txn has amount_usd",  "amount_usd" in first_txn)
check("txn has customer_id", "customer_id" in first_txn)

print("\n/api/transactions (filter by type)")
r = get("/api/transactions", {"txn_type": "cash_deposit", "per_page": 20})
d = r.json()
check("filter by type works", all(t["transaction_type"] == "cash_deposit" for t in d["data"]))

print("\n/api/transactions (filter by amount)")
r = get("/api/transactions", {"min_amount": 9000, "max_amount": 9999, "per_page": 50})
d = r.json()
check("filter by amount works",
      all(9000 <= t["amount"] <= 9999 for t in d["data"]) if d["data"] else True)
check("amount filter returns results", d["total"] > 0)

print("\n/api/transactions (alerted_only)")
r = get("/api/transactions", {"alerted_only": "true", "per_page": 10})
d = r.json()
check("alerted_only returns results", d["total"] > 0)

print("\n/api/transactions (pagination)")
r1 = get("/api/transactions", {"page": 1, "per_page": 10})
r2 = get("/api/transactions", {"page": 2, "per_page": 10})
d1, d2 = r1.json(), r2.json()
ids1 = {t["id"] for t in d1["data"]}
ids2 = {t["id"] for t in d2["data"]}
check("page 2 different from page 1", ids1.isdisjoint(ids2))

# ── /api/alerts ──────────────────────────────────────────────────────────────
print("\n/api/alerts (default)")
r = get("/api/alerts")
check("status 200", r.status_code == 200)
d = r.json()
check("has data", isinstance(d.get("data"), list) and len(d["data"]) > 0)
check("total > 0", d.get("total", 0) > 0)
first_alert = d["data"][0]
check("alert has score",     "score" in first_alert)
check("alert has rule_name", "rule_name" in first_alert)
check("alert has customer",  "customer_id" in first_alert)
check("alerts ordered by score desc",
      all(d["data"][i]["score"] >= d["data"][i+1]["score"] for i in range(min(9, len(d["data"])-1))))

print("\n/api/alerts (filter by status)")
r = get("/api/alerts", {"status": "open", "per_page": 20})
d = r.json()
check("filter status=open works", all(a["status"] == "open" for a in d["data"]))

print("\n/api/alerts (filter by min_score)")
r = get("/api/alerts", {"min_score": 60, "per_page": 20})
d = r.json()
check("min_score filter works", all(a["score"] >= 60 for a in d["data"]))

print("\n/api/alerts (filter by category)")
r = get("/api/alerts", {"category": "structuring", "per_page": 20})
d = r.json()
check("category filter works",
      all(a["category"] == "structuring" for a in d["data"]) if d["data"] else True)

# ── /api/alerts/<id> ─────────────────────────────────────────────────────────
print("\n/api/alerts/1 (detail)")
r = get("/api/alerts/1")
check("status 200", r.status_code == 200)
d = r.json()
check("has rule_description", "rule_description" in d)
check("has enrichment fields", "velocity_1h" in d)
check("has score breakdown",   "rule_score" in d)
check("has account_age_days",  "account_age_days" in d)

print("\n/api/alerts/999999 (not found)")
r = get("/api/alerts/999999")
check("404 for missing alert", r.status_code == 404)

# ── /api/customers/<id> ──────────────────────────────────────────────────────
print("\n/api/customers/1")
r = get("/api/customers/1")
check("status 200", r.status_code == 200)
d = r.json()
check("has name",                 "name" in d)
check("has accounts list",        isinstance(d.get("accounts"), list))
check("has recent_transactions",  isinstance(d.get("recent_transactions"), list))
check("has alert_summary",        isinstance(d.get("alert_summary"), dict))
check("has recent_alerts",        isinstance(d.get("recent_alerts"), list))
check("has country_risk_level",   "country_risk_level" in d)

print("\n/api/customers/999999 (not found)")
r = get("/api/customers/999999")
check("404 for missing customer", r.status_code == 404)

# ── /api/rules ───────────────────────────────────────────────────────────────
print("\n/api/rules")
r = get("/api/rules")
check("status 200", r.status_code == 200)
d = r.json()
check("returns list",          isinstance(d, list))
check("17 rules",              len(d) == 17)
check("has alert_count field", all("alert_count" in rule for rule in d))
check("all rules have alerts", all(rule["alert_count"] > 0 for rule in d))

# ── /api/analytics/timeline ──────────────────────────────────────────────────
print("\n/api/analytics/timeline")
r = get("/api/analytics/timeline")
check("status 200", r.status_code == 200)
d = r.json()
check("12 months",           len(d) == 12)
check("has month field",     all("month" in m for m in d))
check("has transaction_count", all("transaction_count" in m for m in d))
check("has alert_count",     all("alert_count" in m for m in d))
check("December highest",    d[-1]["transaction_count"] > d[0]["transaction_count"])

# ── /api/analytics/risk-distribution ────────────────────────────────────────
print("\n/api/analytics/risk-distribution")
r = get("/api/analytics/risk-distribution")
check("status 200", r.status_code == 200)
d = r.json()
check("3 risk levels",       len(d) == 3)
check("has customers field", all("customers" in row for row in d))
check("has alert count",     all("alerts" in row for row in d))

# ── /api/analytics/rule-performance ─────────────────────────────────────────
print("\n/api/analytics/rule-performance")
r = get("/api/analytics/rule-performance")
check("status 200", r.status_code == 200)
d = r.json()
check("returns list", isinstance(d, list))
check("17 rules",     len(d) == 17)
check("has avg_score", all("avg_score" in row for row in d))
check("no zero-hit rules", all(row["total_alerts"] > 0 for row in d))

# ── /api/analytics/geographic ───────────────────────────────────────────────
print("\n/api/analytics/geographic")
r = get("/api/analytics/geographic")
check("status 200", r.status_code == 200)
d = r.json()
check("returns list", isinstance(d, list))
check("has country entries", len(d) > 5)
check("has total_usd", all("total_usd" in row for row in d))

# ── /api/analytics/top-customers ────────────────────────────────────────────
print("\n/api/analytics/top-customers")
r = get("/api/analytics/top-customers")
check("status 200", r.status_code == 200)
d = r.json()
check("returns list",         isinstance(d, list))
check("up to 20 customers",   1 <= len(d) <= 20)
check("has max_score",        all("max_score" in row for row in d))
check("ordered by max_score", all(d[i]["max_score"] >= d[i+1]["max_score"] for i in range(len(d)-1)))

# ── Summary ──────────────────────────────────────────────────────────────────
total = PASS + FAIL
print(f"\n{'='*60}")
print(f"Results: {PASS}/{total} passed ({100*PASS//total}%)")
if FAIL:
    print(f"FAILURES: {FAIL}")
    sys.exit(1)
else:
    print("All tests passed!")
