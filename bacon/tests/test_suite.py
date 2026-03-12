"""
tests/test_suite.py — Comprehensive test suite for the Transaction Monitoring Platform.

Covers:
  - Schema validation (tables, columns, constraints)
  - Data integrity (FK references, CHECK constraints, nulls)
  - ETL validation (enrichment correctness, coverage)
  - Rule engine verification (alert counts, score ranges, fraud pattern coverage)
  - API contract tests (all 11 endpoints)

Run: python tests/test_suite.py
"""

from __future__ import annotations

import sys
import json
import os
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import sqlite3
import subprocess
import time
import threading

import requests

import db

# ─────────────────────────────────────────────────────────────────────────────
# Test harness
# ─────────────────────────────────────────────────────────────────────────────

PASS = 0
FAIL = 0
ERRORS: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        msg = f"  [FAIL] {name}" + (f" -- {detail}" if detail else "")
        ERRORS.append(msg)
        print(msg)


def section(title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ─────────────────────────────────────────────────────────────────────────────
# Schema tests
# ─────────────────────────────────────────────────────────────────────────────

def test_schema(conn: sqlite3.Connection) -> None:
    section("SCHEMA TESTS")

    # Tables exist
    tables_q = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    table_names = {r['name'] for r in tables_q}

    expected_tables = {
        'customers', 'accounts', 'transactions', 'transaction_enrichment',
        'rules', 'alerts', 'alert_scores', 'audit_log', 'country_risk',
        'rule_performance',
    }
    for tbl in sorted(expected_tables):
        check(f"Table '{tbl}' exists", tbl in table_names)

    # Column counts
    col_counts = {
        'customers':              9,
        'accounts':               8,
        'transactions':           12,
        'transaction_enrichment': 11,
        'rules':                  8,
        'alerts':                 8,
        'alert_scores':           6,
        'country_risk':           5,
        'rule_performance':       11,
    }
    for tbl, expected_n in col_counts.items():
        cols = conn.execute(f"PRAGMA table_info({tbl})").fetchall()
        check(f"  {tbl}: {expected_n} columns", len(cols) == expected_n,
              f"got {len(cols)}")

    # Indexes exist
    indexes = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index'"
    ).fetchall()
    idx_names = {r['name'] for r in indexes}
    expected_indexes = [
        'idx_transactions_account_id', 'idx_transactions_timestamp',
        'idx_customers_risk_rating', 'idx_alerts_score',
        'idx_enrichment_velocity_1h',
    ]
    for idx in expected_indexes:
        check(f"Index '{idx}' exists", idx in idx_names)

    # Foreign key pragma enabled
    (fk_on,) = conn.execute("PRAGMA foreign_keys").fetchone()
    check("Foreign keys ON", fk_on == 1)

    # Integrity check
    result = conn.execute("PRAGMA integrity_check").fetchone()[0]
    check("SQLite integrity_check passes", result == "ok", result)

    # Country risk data
    (n_countries,) = conn.execute("SELECT COUNT(*) FROM country_risk").fetchone()
    check("30+ country risk entries", n_countries >= 30, f"got {n_countries}")

    sanctioned = conn.execute(
        "SELECT COUNT(*) FROM country_risk WHERE risk_level = 5"
    ).fetchone()[0]
    check("Sanctioned countries present (risk=5)", sanctioned > 0)

    # CHECK constraint: risk_rating in (1,2,3)
    try:
        conn.execute("""
            INSERT INTO customers (name, customer_type, risk_rating, country,
                                   registration_date, kyc_status, pep_status)
            VALUES ('Test', 'individual', 99, 'US', '2020-01-01', 'verified', 0)
        """)
        conn.rollback()
        check("CHECK constraint: invalid risk_rating rejected", False, "INSERT succeeded unexpectedly")
    except sqlite3.IntegrityError:
        check("CHECK constraint: invalid risk_rating rejected", True)

    # CHECK constraint: pep_status in (0,1)
    try:
        conn.execute("""
            INSERT INTO customers (name, customer_type, risk_rating, country,
                                   registration_date, kyc_status, pep_status)
            VALUES ('Test', 'individual', 1, 'US', '2020-01-01', 'verified', 99)
        """)
        conn.rollback()
        check("CHECK constraint: invalid pep_status rejected", False, "INSERT succeeded unexpectedly")
    except sqlite3.IntegrityError:
        check("CHECK constraint: invalid pep_status rejected", True)


# ─────────────────────────────────────────────────────────────────────────────
# Data integrity tests
# ─────────────────────────────────────────────────────────────────────────────

def test_data_integrity(conn: sqlite3.Connection) -> None:
    section("DATA INTEGRITY TESTS")

    # Row counts
    (n_cust,) = conn.execute("SELECT COUNT(*) FROM customers").fetchone()
    check(f"2000 customers", n_cust == 2000, f"got {n_cust}")

    (n_acct,) = conn.execute("SELECT COUNT(*) FROM accounts").fetchone()
    check(f"4000+ accounts", n_acct >= 4000, f"got {n_acct}")

    (n_txn,) = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()
    check(f"190,000+ transactions", n_txn >= 190_000, f"got {n_txn}")

    # All accounts reference valid customers
    orphan_accts = conn.execute("""
        SELECT COUNT(*) FROM accounts a
        LEFT JOIN customers c ON c.id = a.customer_id
        WHERE c.id IS NULL
    """).fetchone()[0]
    check("No orphan accounts", orphan_accts == 0)

    # All transactions reference valid accounts
    orphan_txns = conn.execute("""
        SELECT COUNT(*) FROM transactions t
        LEFT JOIN accounts a ON a.id = t.account_id
        WHERE a.id IS NULL
    """).fetchone()[0]
    check("No orphan transactions", orphan_txns == 0)

    # All transactions reference valid currencies
    bad_currency = conn.execute("""
        SELECT COUNT(*) FROM transactions
        WHERE length(currency) != 3
    """).fetchone()[0]
    check("All transaction currencies are 3-char", bad_currency == 0)

    # All amounts positive
    neg_amounts = conn.execute(
        "SELECT COUNT(*) FROM transactions WHERE amount <= 0"
    ).fetchone()[0]
    check("All transaction amounts > 0", neg_amounts == 0)

    # All timestamps in 2024
    bad_ts = conn.execute("""
        SELECT COUNT(*) FROM transactions
        WHERE timestamp < '2024-01-01' OR timestamp > '2025-01-01'
    """).fetchone()[0]
    check("All timestamps in year 2024", bad_ts == 0, f"{bad_ts} out of range")

    # Customers have non-null names
    null_names = conn.execute(
        "SELECT COUNT(*) FROM customers WHERE name IS NULL OR name = ''"
    ).fetchone()[0]
    check("No null/empty customer names", null_names == 0)

    # Every account has a valid customer_type
    valid_types = {'individual', 'business', 'financial_institution'}
    bad_types = conn.execute("""
        SELECT COUNT(*) FROM customers
        WHERE customer_type NOT IN ('individual', 'business', 'financial_institution')
    """).fetchone()[0]
    check("All customer types valid", bad_types == 0)

    # Accounts opened after customer registered
    late_accounts = conn.execute("""
        SELECT COUNT(*) FROM accounts a
        JOIN customers c ON c.id = a.customer_id
        WHERE a.opened_date < c.registration_date
    """).fetchone()[0]
    check("Account opened_date >= customer registration_date",
          late_accounts == 0, f"{late_accounts} violations")

    # Fraud pattern metadata in audit log
    fraud_log = conn.execute("""
        SELECT metadata FROM audit_log
        WHERE event_type = 'data_loaded'
        LIMIT 1
    """).fetchone()
    check("Fraud pattern metadata in audit log", fraud_log is not None)
    if fraud_log:
        meta = json.loads(fraud_log['metadata'])
        check("Fraud customers recorded in metadata",
              "fraud_customers" in meta and len(meta["fraud_customers"]) == 8)


# ─────────────────────────────────────────────────────────────────────────────
# ETL validation tests
# ─────────────────────────────────────────────────────────────────────────────

def test_etl(conn: sqlite3.Connection) -> None:
    section("ETL VALIDATION TESTS")

    (n_txn,)  = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()
    (n_enr,)  = conn.execute("SELECT COUNT(*) FROM transaction_enrichment").fetchone()
    check("100% ETL coverage", n_txn == n_enr, f"{n_enr}/{n_txn}")

    # All amount_usd values positive
    bad_usd = conn.execute(
        "SELECT COUNT(*) FROM transaction_enrichment WHERE amount_usd <= 0"
    ).fetchone()[0]
    check("All amount_usd > 0", bad_usd == 0)

    # Large cash only on cash transactions
    wrong_large_cash = conn.execute("""
        SELECT COUNT(*) FROM transaction_enrichment te
        JOIN transactions t ON t.id = te.transaction_id
        WHERE te.is_large_cash = 1
          AND t.transaction_type NOT IN ('cash_deposit', 'cash_withdrawal')
    """).fetchone()[0]
    check("is_large_cash only set on cash transactions", wrong_large_cash == 0)

    # Large cash threshold is correct (>= 10000 USD)
    wrong_threshold = conn.execute("""
        SELECT COUNT(*) FROM transaction_enrichment
        WHERE is_large_cash = 1 AND amount_usd < 10000
    """).fetchone()[0]
    check("Large cash threshold >= 10,000 USD", wrong_threshold == 0)

    # FX: USD transactions should have amount_usd == amount
    usd_mismatch = conn.execute("""
        SELECT COUNT(*) FROM transaction_enrichment te
        JOIN transactions t ON t.id = te.transaction_id
        WHERE t.currency = 'USD'
          AND ABS(te.amount_usd - t.amount) > 0.01
    """).fetchone()[0]
    check("USD transactions: amount_usd == amount", usd_mismatch == 0,
          f"{usd_mismatch} mismatches")

    # Velocity values non-negative
    bad_vel = conn.execute("""
        SELECT COUNT(*) FROM transaction_enrichment
        WHERE velocity_1h < 0 OR velocity_24h < 0 OR velocity_7d < 0
    """).fetchone()[0]
    check("Velocity values non-negative", bad_vel == 0)

    # Ordering: velocity_1h <= velocity_24h <= velocity_7d
    vel_order = conn.execute("""
        SELECT COUNT(*) FROM transaction_enrichment
        WHERE velocity_1h > velocity_24h OR velocity_24h > velocity_7d
    """).fetchone()[0]
    check("Velocity ordering: 1h <= 24h <= 7d", vel_order == 0,
          f"{vel_order} violations")

    # Country risk scores in [1,5]
    bad_risk = conn.execute("""
        SELECT COUNT(*) FROM transaction_enrichment
        WHERE country_risk_score NOT BETWEEN 1 AND 5
    """).fetchone()[0]
    check("Country risk scores in range [1,5]", bad_risk == 0)

    # Structuring detection: our fraud deposits should be findable
    struct_count = conn.execute("""
        SELECT COUNT(*) FROM transactions t
        JOIN transaction_enrichment te ON te.transaction_id = t.id
        WHERE t.transaction_type = 'cash_deposit'
          AND t.amount BETWEEN 9000 AND 9999
    """).fetchone()[0]
    check("Structuring deposits detected (>= 100)", struct_count >= 100,
          f"found {struct_count}")

    # Rapid velocity: should have accounts with velocity_1h >= 20
    high_vel = conn.execute(
        "SELECT MAX(velocity_1h) FROM transaction_enrichment"
    ).fetchone()[0]
    check("Rapid velocity fraud detected (max vel_1h >= 20)", high_vel >= 20,
          f"max={high_vel}")

    # Account age non-negative
    neg_age = conn.execute(
        "SELECT COUNT(*) FROM transaction_enrichment WHERE account_age_days < 0"
    ).fetchone()[0]
    check("Account age non-negative", neg_age == 0)


# ─────────────────────────────────────────────────────────────────────────────
# Rule engine verification tests
# ─────────────────────────────────────────────────────────────────────────────

def test_rule_engine(conn: sqlite3.Connection) -> None:
    section("RULE ENGINE TESTS")

    (n_rules,) = conn.execute("SELECT COUNT(*) FROM rules").fetchone()
    check("17 rules seeded", n_rules == 17, f"got {n_rules}")

    (active,) = conn.execute("SELECT COUNT(*) FROM rules WHERE is_active=1").fetchone()
    check("All 17 rules active", active == 17)

    (n_alerts,) = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()
    check("40,000+ alerts generated", n_alerts >= 40_000, f"got {n_alerts}")

    # No rule with zero hits
    zero_hit = conn.execute("""
        SELECT r.name FROM rules r
        LEFT JOIN alerts a ON a.rule_id = r.id
        WHERE r.is_active = 1
        GROUP BY r.id
        HAVING COUNT(a.id) = 0
    """).fetchall()
    check("No zero-hit rules", len(zero_hit) == 0,
          f"zero-hit: {[r['name'] for r in zero_hit]}")

    # Score range
    (min_s, max_s) = conn.execute(
        "SELECT MIN(score), MAX(score) FROM alerts"
    ).fetchone()
    check("Alert scores in [0, 100]",
          min_s >= 0 and max_s <= 100, f"range=[{min_s},{max_s}]")

    # Sanctioned country rule should have low count but high score
    sanc_alerts = conn.execute("""
        SELECT COUNT(*), AVG(score) FROM alerts al
        JOIN rules r ON r.id = al.rule_id
        WHERE r.name = 'GEO-001 Sanctioned country transaction'
    """).fetchone()
    check("Sanctioned country alerts exist", sanc_alerts[0] > 0)
    if sanc_alerts[0] > 0:
        check("Sanctioned alerts high score (avg >= 55)",
              sanc_alerts[1] >= 55, f"avg={sanc_alerts[1]:.1f}")

    # Structuring rule
    struct_alerts = conn.execute("""
        SELECT COUNT(*) FROM alerts al
        JOIN rules r ON r.id = al.rule_id
        WHERE r.name = 'STR-001 Sub-threshold cash deposit'
    """).fetchone()[0]
    check("Structuring rule STR-001 has alerts", struct_alerts > 0,
          f"count={struct_alerts}")

    # Velocity rule
    vel_alerts = conn.execute("""
        SELECT COUNT(*) FROM alerts al
        JOIN rules r ON r.id = al.rule_id
        WHERE r.name = 'VEL-001 Rapid transaction burst'
    """).fetchone()[0]
    check("Velocity rule VEL-001 has alerts", vel_alerts > 0)

    # Alert-scores completeness
    (n_alerted,) = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()
    (n_scored,)  = conn.execute("SELECT COUNT(*) FROM alert_scores").fetchone()
    check("Every alert has a score breakdown",
          n_alerted == n_scored, f"{n_scored}/{n_alerted}")

    # Composite score formula sanity check (sample 10 alerts)
    rows = conn.execute("""
        SELECT asc2.rule_score, asc2.customer_risk_score,
               asc2.geographic_risk_score, asc2.behavioral_score,
               asc2.composite_score
        FROM alert_scores asc2
        LIMIT 10
    """).fetchall()
    for r in rows:
        expected = (
            0.30 * r['rule_score']
            + 0.25 * r['customer_risk_score']
            + 0.25 * r['geographic_risk_score']
            + 0.20 * r['behavioral_score']
        ) * 100
        diff = abs(expected - r['composite_score'])
        check(f"  Score formula correct (diff={diff:.2f})", diff < 0.1, f"{diff}")

    # Category coverage
    categories = conn.execute("""
        SELECT DISTINCT r.category FROM rules r ORDER BY r.category
    """).fetchall()
    cats = {r['category'] for r in categories}
    for cat in ['structuring', 'velocity', 'geographic', 'behavioral', 'threshold']:
        check(f"Category '{cat}' has rules", cat in cats)

    # Unique constraint: no duplicate (transaction_id, rule_id) alerts
    dupes = conn.execute("""
        SELECT transaction_id, rule_id, COUNT(*) n
        FROM alerts
        GROUP BY transaction_id, rule_id
        HAVING n > 1
    """).fetchall()
    check("No duplicate (transaction_id, rule_id) alerts", len(dupes) == 0,
          f"{len(dupes)} duplicates")


# ─────────────────────────────────────────────────────────────────────────────
# API tests (requires running server)
# ─────────────────────────────────────────────────────────────────────────────

BASE = "http://127.0.0.1:5000"


def test_api() -> None:
    section("API TESTS")

    def get(path, **params):
        try:
            return requests.get(BASE + path, params=params or None, timeout=8)
        except requests.ConnectionError:
            return None

    # Check if server is up
    r = get("/api/stats")
    if r is None:
        print("  [SKIP] API server not running — skipping API tests")
        return

    check("Server reachable", r.status_code == 200)
    d = r.json()

    # /api/stats
    check("/api/stats: customers=2000", d.get("customers") == 2000)
    check("/api/stats: transactions>=190k", d.get("transactions", 0) >= 190_000)
    check("/api/stats: alerts>=40k",       d.get("alerts", 0) >= 40_000)

    # /api/transactions pagination
    r = get("/api/transactions", page=1, per_page=10)
    check("/api/transactions 200", r.status_code == 200)
    d = r.json()
    check("/api/transactions: 10 items returned", len(d["data"]) == 10)
    check("/api/transactions: total matches DB",
          abs(d["total"] - 198_891) < 1000)

    # /api/alerts filter
    r = get("/api/alerts", min_score=60)
    check("/api/alerts min_score filter", r.status_code == 200)
    d = r.json()
    check("/api/alerts: all scores >= 60",
          all(a["score"] >= 60 for a in d["data"]))

    # /api/alerts/<id>
    r = get("/api/alerts/1")
    check("/api/alerts/1: 200", r.status_code == 200)
    d = r.json()
    for field in ["rule_description", "velocity_1h", "rule_score", "score"]:
        check(f"/api/alerts/1: has {field}", field in d)

    # /api/customers/<id>
    r = get("/api/customers/1")
    check("/api/customers/1: 200", r.status_code == 200)
    d = r.json()
    check("/api/customers/1: has accounts", isinstance(d.get("accounts"), list))

    # /api/rules
    r = get("/api/rules")
    d = r.json()
    check("/api/rules: 17 rules", len(d) == 17)

    # Analytics endpoints
    for path in ["/api/analytics/timeline", "/api/analytics/risk-distribution",
                 "/api/analytics/rule-performance", "/api/analytics/geographic",
                 "/api/analytics/top-customers"]:
        r = get(path)
        check(f"{path}: 200", r.status_code == 200)
        d = r.json()
        check(f"{path}: non-empty", len(d) > 0)


# ─────────────────────────────────────────────────────────────────────────────
# Populate rule_performance table
# ─────────────────────────────────────────────────────────────────────────────

def populate_rule_performance(conn: sqlite3.Connection) -> None:
    section("RULE PERFORMANCE TABLE")

    # Load fraud customer IDs from audit log
    fraud_meta_row = conn.execute("""
        SELECT metadata FROM audit_log
        WHERE event_type = 'data_loaded' LIMIT 1
    """).fetchone()
    fraud_customers: dict[str, list[int]] = {}
    if fraud_meta_row:
        meta = json.loads(fraud_meta_row['metadata'])
        fraud_customers = {k: list(map(int, v)) for k, v in meta.get("fraud_customers", {}).items()}

    all_fraud_ids = set()
    for v in fraud_customers.values():
        all_fraud_ids.update(v)

    eval_date = "2024-12-31"

    rules = conn.execute("SELECT * FROM rules WHERE is_active = 1").fetchall()
    inserted = 0
    for rule in rules:
        alerts = conn.execute("""
            SELECT al.id, al.score, c.id AS customer_id
            FROM alerts al
            JOIN transactions t ON t.id = al.transaction_id
            JOIN accounts a ON a.id = t.account_id
            JOIN customers c ON c.id = a.customer_id
            WHERE al.rule_id = ?
        """, (rule['id'],)).fetchall()

        if not alerts:
            continue

        scores = [a['score'] for a in alerts]
        total  = len(scores)
        # Ground truth: TP = alert on a known fraud customer
        tp = sum(1 for a in alerts if a['customer_id'] in all_fraud_ids)
        fp = total - tp
        precision = round(tp / total, 4) if total else None
        avg_score = round(sum(scores) / total, 2) if scores else None

        sorted_scores = sorted(scores)
        n = len(sorted_scores)
        median_score = sorted_scores[n // 2] if n else None
        p95_score    = sorted_scores[int(0.95 * n)] if n else None

        conn.execute("""
            INSERT OR REPLACE INTO rule_performance
              (rule_id, evaluation_date, total_alerts, true_positives, false_positives,
               precision_rate, avg_score, median_score, p95_score,
               notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            rule['id'], eval_date, total, tp, fp, precision,
            avg_score, median_score, p95_score,
            f"Ground truth: {len(all_fraud_ids)} known fraud customers"
        ))
        inserted += 1

    conn.commit()
    print(f"\n  Populated rule_performance for {inserted} rules.")

    # Print precision table
    rows = conn.execute("""
        SELECT r.name, rp.total_alerts, rp.true_positives, rp.false_positives,
               rp.precision_rate, rp.avg_score
        FROM rule_performance rp
        JOIN rules r ON r.id = rp.rule_id
        ORDER BY rp.precision_rate DESC NULLS LAST
    """).fetchall()

    print(f"\n  {'Rule':<45} {'Alerts':>8} {'TP':>6} {'FP':>6} {'Prec':>7} {'AvgScore':>9}")
    print("  " + "-" * 85)
    for r in rows:
        prec = f"{r['precision_rate']:.2%}" if r['precision_rate'] is not None else "N/A"
        print(f"  {r['name']:<45} {r['total_alerts']:>8,} {r['true_positives']:>6} "
              f"{r['false_positives']:>6} {prec:>7} {r['avg_score']:>9.1f}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 60)
    print("Transaction Monitor — Comprehensive Test Suite")
    print("=" * 60)

    conn = db.get_connection()
    try:
        test_schema(conn)
        test_data_integrity(conn)
        test_etl(conn)
        test_rule_engine(conn)
        populate_rule_performance(conn)
        test_api()
    finally:
        conn.close()

    total = PASS + FAIL
    print(f"\n{'='*60}")
    print(f"Results: {PASS}/{total} passed ({100*PASS//total if total else 0}%)")
    if FAIL:
        print(f"FAILURES ({FAIL}):")
        for e in ERRORS:
            print(f"  {e}")
        sys.exit(1)
    else:
        print("All tests passed!")


if __name__ == "__main__":
    main()
