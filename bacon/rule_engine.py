"""
rule_engine.py — Configurable AML rule engine with composite scoring.

Loads 15+ rules from the database (or seeds them on first run), evaluates
each rule against the transaction/enrichment data, and generates alerts
with a composite risk score:

    composite = 100 × (0.30 × rule_severity
                      + 0.25 × customer_risk_norm
                      + 0.25 × geographic_risk_norm
                      + 0.20 × behavioral_score)
    clamped to [0, 100].
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import db

# ─────────────────────────────────────────────────────────────────────────────
# Rule definitions (seeded into DB on first run)
# ─────────────────────────────────────────────────────────────────────────────

RULE_DEFINITIONS: list[dict[str, Any]] = [
    # ── STRUCTURING ──────────────────────────────────────────────────────────
    {
        "name": "STR-001 Sub-threshold cash deposit",
        "description": "Single cash deposit just below the $10,000 CTR reporting threshold ($9,000–$9,999).",
        "category": "structuring",
        "severity": 0.80,
        "sql_condition": """
            t.transaction_type = 'cash_deposit'
            AND te.amount_usd BETWEEN 9000 AND 9999.99
        """,
    },
    {
        "name": "STR-002 Aggregate daily sub-threshold deposits",
        "description": "Customer deposits more than $9,000 in cash across multiple transactions in a single day, each below $10,000.",
        "category": "structuring",
        "severity": 0.85,
        "sql_condition": """
            t.transaction_type IN ('cash_deposit')
            AND te.amount_velocity_24h > 9000
            AND te.amount_usd < 10000
        """,
    },
    {
        "name": "STR-003 Cross-account smurfing",
        "description": "Customer has multiple accounts each receiving sub-$10,000 cash deposits on the same day, totaling over $20,000.",
        "category": "structuring",
        "severity": 0.90,
        "sql_condition": """
            t.transaction_type = 'cash_deposit'
            AND te.amount_usd BETWEEN 8500 AND 9999.99
            AND te.velocity_24h >= 2
        """,
    },
    {
        "name": "STR-004 Declining cash deposit pattern",
        "description": "Cash deposits decreasing in size over multiple transactions (testing reporting thresholds).",
        "category": "structuring",
        "severity": 0.75,
        "sql_condition": """
            t.transaction_type = 'cash_deposit'
            AND te.amount_usd BETWEEN 7000 AND 9999.99
            AND te.velocity_7d >= 5
        """,
    },
    # ── VELOCITY ─────────────────────────────────────────────────────────────
    {
        "name": "VEL-001 Rapid transaction burst",
        "description": "20 or more transactions from the same account within a 1-hour window.",
        "category": "velocity",
        "severity": 0.85,
        "sql_condition": """
            te.velocity_1h >= 20
        """,
    },
    {
        "name": "VEL-002 High 24h transaction count",
        "description": "More than 40 transactions on a single account within any 24-hour period.",
        "category": "velocity",
        "severity": 0.70,
        "sql_condition": """
            te.velocity_24h >= 40
        """,
    },
    {
        "name": "VEL-003 Volume spike — 24h amount anomaly",
        "description": "Account moves more than $100,000 USD equivalent within 24 hours (unusual for account profile).",
        "category": "velocity",
        "severity": 0.75,
        "sql_condition": """
            te.amount_velocity_24h > 100000
        """,
    },
    # ── GEOGRAPHIC ───────────────────────────────────────────────────────────
    {
        "name": "GEO-001 Sanctioned country transaction",
        "description": "Transaction involving a counterparty in a comprehensively sanctioned country (risk level 5).",
        "category": "geographic",
        "severity": 0.95,
        "sql_condition": """
            te.country_risk_score = 5
        """,
    },
    {
        "name": "GEO-002 Very high risk country",
        "description": "Transaction involving a counterparty in a very-high-risk jurisdiction (risk level 4).",
        "category": "geographic",
        "severity": 0.80,
        "sql_condition": """
            te.country_risk_score = 4
        """,
    },
    {
        "name": "GEO-003 High-risk country new counterparty",
        "description": "First-ever transaction to a new counterparty in a high-risk country (risk >= 3).",
        "category": "geographic",
        "severity": 0.70,
        "sql_condition": """
            te.country_risk_score >= 3
            AND te.is_new_counterparty = 1
        """,
    },
    # ── BEHAVIORAL ───────────────────────────────────────────────────────────
    {
        "name": "BEH-001 Unusual large amount",
        "description": "Single transaction amount is more than 10x the account's typical transaction (amount_usd > 50,000 on individual accounts).",
        "category": "behavioral",
        "severity": 0.80,
        "sql_condition": """
            te.amount_usd > 50000
            AND c.customer_type = 'individual'
        """,
    },
    {
        "name": "BEH-002 Dormant account sudden activity",
        "description": "Account with no transactions for 180+ days suddenly initiates a high-value transaction.",
        "category": "behavioral",
        "severity": 0.75,
        "sql_condition": """
            te.velocity_7d = 0
            AND te.amount_usd > 10000
        """,
    },
    {
        "name": "BEH-003 Round-amount large wire",
        "description": "Large round-amount wire transfer, often indicative of structured layering.",
        "category": "behavioral",
        "severity": 0.65,
        "sql_condition": """
            t.transaction_type IN ('wire_out', 'wire_in')
            AND te.is_round_amount = 1
            AND te.amount_usd > 25000
        """,
    },
    # ── THRESHOLD ────────────────────────────────────────────────────────────
    {
        "name": "THR-001 Large cash transaction",
        "description": "Cash transaction exceeding $10,000 USD equivalent (CTR reporting threshold).",
        "category": "threshold",
        "severity": 0.60,
        "sql_condition": """
            te.is_large_cash = 1
        """,
    },
    {
        "name": "THR-002 Large international wire",
        "description": "International wire transfer exceeding $50,000 USD equivalent.",
        "category": "threshold",
        "severity": 0.65,
        "sql_condition": """
            t.transaction_type IN ('wire_out', 'wire_in')
            AND te.amount_usd > 50000
            AND t.counterparty_country IS NOT NULL
        """,
    },
    # ── EXTRA ────────────────────────────────────────────────────────────────
    {
        "name": "BEH-004 PEP high-value transaction",
        "description": "Politically exposed person (PEP) involved in a transaction over $25,000.",
        "category": "behavioral",
        "severity": 0.85,
        "sql_condition": """
            c.pep_status = 1
            AND te.amount_usd > 25000
        """,
    },
    {
        "name": "GEO-004 Multi-country activity — new countries",
        "description": "Account transacts with new counterparties in 3+ different countries within 7 days.",
        "category": "geographic",
        "severity": 0.70,
        "sql_condition": """
            te.is_new_counterparty = 1
            AND te.country_risk_score >= 2
            AND te.velocity_7d >= 5
        """,
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# Data class for rule result
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class AlertRecord:
    """Holds all fields needed to insert an alert + score."""
    transaction_id:         int
    rule_id:                int
    rule_score:             float      # 0.0–1.0 (= severity)
    customer_risk_score:    float      # 0.0–1.0
    geographic_risk_score:  float      # 0.0–1.0
    behavioral_score:       float      # 0.0–1.0
    composite_score:        float      # 0.0–100.0


# ─────────────────────────────────────────────────────────────────────────────
# Seed rules
# ─────────────────────────────────────────────────────────────────────────────

def seed_rules(conn: sqlite3.Connection) -> None:
    """Insert RULE_DEFINITIONS into the rules table if not already present.

    Args:
        conn: Open database connection.
    """
    existing = {
        row['name']
        for row in conn.execute("SELECT name FROM rules").fetchall()
    }
    new_rules = [r for r in RULE_DEFINITIONS if r['name'] not in existing]
    if new_rules:
        conn.executemany("""
            INSERT INTO rules (name, description, category, sql_condition, severity, is_active)
            VALUES (:name, :description, :category, :sql_condition, :severity, 1)
        """, new_rules)
        conn.commit()
        print(f"[rules] Seeded {len(new_rules)} new rules.")
    else:
        print(f"[rules] All {len(existing)} rules already in DB.")


# ─────────────────────────────────────────────────────────────────────────────
# Scoring helpers
# ─────────────────────────────────────────────────────────────────────────────

def normalize_customer_risk(risk_rating: int) -> float:
    """Map customer risk rating (1–3) to a 0.0–1.0 score.

    Args:
        risk_rating: Integer 1 (low) to 3 (high).

    Returns:
        Float in [0.0, 1.0].
    """
    return {1: 0.2, 2: 0.6, 3: 1.0}.get(risk_rating, 0.2)


def normalize_geo_risk(country_risk_score: int) -> float:
    """Map country risk level (1–5) to a 0.0–1.0 geographic risk score.

    Args:
        country_risk_score: Integer 1 (low) to 5 (sanctioned).

    Returns:
        Float in [0.0, 1.0].
    """
    return {1: 0.1, 2: 0.35, 3: 0.6, 4: 0.85, 5: 1.0}.get(country_risk_score, 0.1)


def compute_behavioral_score(
    velocity_1h: int,
    velocity_24h: int,
    amount_usd: float,
    amount_velocity_24h: float,
    is_round: bool,
    is_new_cp: bool,
) -> float:
    """Compute a behavioral anomaly score in [0.0, 1.0].

    Combines multiple soft signals:
      - Velocity spikes
      - Amount size relative to typical transactions
      - Round amount + new counterparty combination

    Args:
        velocity_1h:           Transaction count in past hour.
        velocity_24h:          Transaction count in past 24 hours.
        amount_usd:            Amount in USD.
        amount_velocity_24h:   Total USD moved in past 24 hours.
        is_round:              Whether the amount is a round number.
        is_new_cp:             Whether the counterparty is new.

    Returns:
        Float behavioral score in [0.0, 1.0].
    """
    score = 0.0

    # Velocity component (max 0.4)
    if velocity_1h >= 20:
        score += 0.40
    elif velocity_1h >= 10:
        score += 0.25
    elif velocity_1h >= 5:
        score += 0.10
    elif velocity_24h >= 40:
        score += 0.20
    elif velocity_24h >= 20:
        score += 0.10

    # Amount component (max 0.35)
    if amount_usd >= 500_000:
        score += 0.35
    elif amount_usd >= 100_000:
        score += 0.25
    elif amount_usd >= 50_000:
        score += 0.15
    elif amount_usd >= 10_000:
        score += 0.08

    # Round amount + new counterparty (max 0.15)
    if is_round and is_new_cp:
        score += 0.15
    elif is_round:
        score += 0.07
    elif is_new_cp:
        score += 0.05

    # Daily volume (max 0.10)
    if amount_velocity_24h >= 100_000:
        score += 0.10
    elif amount_velocity_24h >= 25_000:
        score += 0.05

    return min(1.0, score)


def composite_score(
    rule_severity:         float,
    customer_risk_norm:    float,
    geographic_risk_norm:  float,
    behavioral:            float,
) -> float:
    """Compute the composite alert score on a 0–100 scale.

    Formula:  100 × (0.30×rule + 0.25×customer + 0.25×geo + 0.20×behavioral)

    Args:
        rule_severity:        Rule severity in [0, 1].
        customer_risk_norm:   Normalized customer risk in [0, 1].
        geographic_risk_norm: Normalized geographic risk in [0, 1].
        behavioral:           Behavioral score in [0, 1].

    Returns:
        Score in [0.0, 100.0].
    """
    raw = (
        0.30 * rule_severity
        + 0.25 * customer_risk_norm
        + 0.25 * geographic_risk_norm
        + 0.20 * behavioral
    )
    return round(min(100.0, max(0.0, raw * 100.0)), 2)


# ─────────────────────────────────────────────────────────────────────────────
# Rule evaluation
# ─────────────────────────────────────────────────────────────────────────────

def evaluate_rule(
    conn: sqlite3.Connection,
    rule: sqlite3.Row,
    existing_alert_keys: set[tuple[int, int]],
) -> list[AlertRecord]:
    """Evaluate a single rule against all enriched transactions.

    Builds a full SELECT statement joining transactions, enrichment,
    accounts, and customers filtered by the rule's sql_condition.

    Args:
        conn:                  Open database connection.
        rule:                  Rule row from the rules table.
        existing_alert_keys:   Set of (transaction_id, rule_id) already alerted.

    Returns:
        List of AlertRecord instances for any matching transactions.
    """
    condition = rule['sql_condition'].strip()

    query = f"""
        SELECT
            t.id            AS txn_id,
            t.account_id,
            t.transaction_type,
            te.amount_usd,
            te.velocity_1h,
            te.velocity_24h,
            te.velocity_7d,
            te.amount_velocity_24h,
            te.country_risk_score,
            te.is_new_counterparty,
            te.is_round_amount,
            c.risk_rating,
            c.customer_type,
            c.pep_status
        FROM transactions t
        JOIN transaction_enrichment te ON te.transaction_id = t.id
        JOIN accounts a                ON a.id = t.account_id
        JOIN customers c               ON c.id = a.customer_id
        WHERE {condition}
          AND t.status = 'completed'
    """

    try:
        matches = conn.execute(query).fetchall()
    except sqlite3.OperationalError as e:
        print(f"  [WARN] Rule '{rule['name']}' query error: {e}")
        return []

    records: list[AlertRecord] = []
    for row in matches:
        key = (row['txn_id'], rule['id'])
        if key in existing_alert_keys:
            continue

        cust_risk = normalize_customer_risk(row['risk_rating'])
        geo_risk  = normalize_geo_risk(row['country_risk_score'])
        beh_score = compute_behavioral_score(
            velocity_1h=row['velocity_1h'],
            velocity_24h=row['velocity_24h'],
            amount_usd=row['amount_usd'],
            amount_velocity_24h=row['amount_velocity_24h'],
            is_round=bool(row['is_round_amount']),
            is_new_cp=bool(row['is_new_counterparty']),
        )
        comp = composite_score(rule['severity'], cust_risk, geo_risk, beh_score)

        records.append(AlertRecord(
            transaction_id=row['txn_id'],
            rule_id=rule['id'],
            rule_score=rule['severity'],
            customer_risk_score=cust_risk,
            geographic_risk_score=geo_risk,
            behavioral_score=beh_score,
            composite_score=comp,
        ))

    return records


# ─────────────────────────────────────────────────────────────────────────────
# Alert insertion
# ─────────────────────────────────────────────────────────────────────────────

def insert_alerts(conn: sqlite3.Connection, records: list[AlertRecord]) -> int:
    """Bulk-insert AlertRecord instances into alerts + alert_scores tables.

    Args:
        conn:    Open database connection.
        records: Alert records to insert.

    Returns:
        Number of alerts inserted.
    """
    if not records:
        return 0

    # Insert alerts
    alert_rows = [
        (r.transaction_id, r.rule_id, r.composite_score, 'open',
         datetime.now().isoformat(timespec='seconds'))
        for r in records
    ]
    conn.executemany("""
        INSERT OR IGNORE INTO alerts
          (transaction_id, rule_id, score, status, created_date)
        VALUES (?, ?, ?, ?, ?)
    """, alert_rows)
    conn.commit()

    # Retrieve the alert IDs we just inserted to link to alert_scores
    # Use ROWID mapping: fetch by (transaction_id, rule_id) pairs
    score_rows = []
    for r in records:
        row = conn.execute("""
            SELECT id FROM alerts
            WHERE transaction_id = ? AND rule_id = ?
        """, (r.transaction_id, r.rule_id)).fetchone()
        if row:
            score_rows.append((
                row['id'],
                r.rule_score,
                r.customer_risk_score,
                r.geographic_risk_score,
                r.behavioral_score,
                r.composite_score,
            ))

    conn.executemany("""
        INSERT OR IGNORE INTO alert_scores
          (alert_id, rule_score, customer_risk_score, geographic_risk_score,
           behavioral_score, composite_score)
        VALUES (?, ?, ?, ?, ?, ?)
    """, score_rows)
    conn.commit()

    return len(records)


# ─────────────────────────────────────────────────────────────────────────────
# Rule performance summary
# ─────────────────────────────────────────────────────────────────────────────

def print_rule_summary(conn: sqlite3.Connection) -> None:
    """Print a summary table of alerts generated per rule.

    Args:
        conn: Open database connection.
    """
    print("\n" + "=" * 70)
    print("RULE ENGINE SUMMARY")
    print("=" * 70)

    rows = conn.execute("""
        SELECT
            r.id,
            r.name,
            r.category,
            r.severity,
            COUNT(a.id)               AS total_alerts,
            ROUND(AVG(a.score), 1)    AS avg_score,
            ROUND(MIN(a.score), 1)    AS min_score,
            ROUND(MAX(a.score), 1)    AS max_score
        FROM rules r
        LEFT JOIN alerts a ON a.rule_id = r.id
        WHERE r.is_active = 1
        GROUP BY r.id
        ORDER BY r.category, r.name
    """).fetchall()

    print(f"\n  {'Rule':<45} {'Cat':<12} {'Sev':>5} {'Alerts':>8} {'Avg':>6} {'Min':>6} {'Max':>6}")
    print("  " + "-" * 95)
    zero_hit_rules = []
    for r in rows:
        flag = " *** ZERO HITS ***" if r['total_alerts'] == 0 else ""
        print(f"  {r['name']:<45} {r['category']:<12} {r['severity']:>5.2f} "
              f"{r['total_alerts']:>8,} {str(r['avg_score'] or '-'):>6} "
              f"{str(r['min_score'] or '-'):>6} {str(r['max_score'] or '-'):>6}{flag}")
        if r['total_alerts'] == 0:
            zero_hit_rules.append(r['name'])

    # Overall stats
    (total_alerts,) = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()
    (unique_txns,)  = conn.execute("SELECT COUNT(DISTINCT transaction_id) FROM alerts").fetchone()
    (unique_custs,) = conn.execute("""
        SELECT COUNT(DISTINCT c.id)
        FROM alerts al
        JOIN transactions t ON t.id = al.transaction_id
        JOIN accounts a ON a.id = t.account_id
        JOIN customers c ON c.id = a.customer_id
    """).fetchone()

    print(f"\n  Total alerts:              {total_alerts:,}")
    print(f"  Unique transactions:       {unique_txns:,}")
    print(f"  Unique customers alerted:  {unique_custs:,}")

    # Score distribution
    rows2 = conn.execute("""
        SELECT
            CASE
                WHEN score >= 80 THEN '80-100 (Critical)'
                WHEN score >= 60 THEN '60-79  (High)'
                WHEN score >= 40 THEN '40-59  (Medium)'
                ELSE                   '0-39   (Low)'
            END AS band,
            COUNT(*) n
        FROM alerts
        GROUP BY band
        ORDER BY band DESC
    """).fetchall()
    print("\n  Alert score distribution:")
    for r in rows2:
        print(f"    {r['band']}: {r['n']:,}")

    if zero_hit_rules:
        print(f"\n  [!] ZERO-HIT RULES: {zero_hit_rules}")
    else:
        print("\n  [OK] All rules generated at least one alert.")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def run_rule_engine(conn: sqlite3.Connection, verbose: bool = True) -> int:
    """Seed rules, evaluate all active rules, insert alerts.

    Args:
        conn:    Open database connection.
        verbose: Whether to print per-rule progress.

    Returns:
        Total number of new alerts generated.
    """
    seed_rules(conn)

    # Load active rules
    rules = conn.execute(
        "SELECT * FROM rules WHERE is_active = 1 ORDER BY category, name"
    ).fetchall()
    print(f"[engine] Evaluating {len(rules)} active rules...")

    # Cache existing (txn_id, rule_id) pairs to avoid duplicate alerts
    existing = {
        (row[0], row[1])
        for row in conn.execute("SELECT transaction_id, rule_id FROM alerts").fetchall()
    }

    total_new = 0
    for rule in rules:
        t0 = time.time()
        records = evaluate_rule(conn, rule, existing)
        n_inserted = insert_alerts(conn, records)
        elapsed = time.time() - t0
        # Update existing set
        for r in records:
            existing.add((r.transaction_id, r.rule_id))
        total_new += n_inserted
        if verbose:
            print(f"  {rule['name']:<45}  hits={n_inserted:>6,}  ({elapsed:.1f}s)")

    return total_new


def main() -> None:
    """Entry point."""
    print("=" * 60)
    print("Transaction Monitor — Rule Engine")
    print("=" * 60)

    conn = db.get_connection()
    try:
        total = run_rule_engine(conn, verbose=True)
        print(f"\n[engine] Done. {total:,} new alerts generated.")

        conn.execute("""
            INSERT INTO audit_log (event_type, description, metadata)
            VALUES ('rule_engine_run', 'Rule engine evaluation complete', ?)
        """, (json.dumps({"new_alerts": total}),))
        conn.commit()

        print_rule_summary(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
