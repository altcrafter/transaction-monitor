"""
etl.py — ETL pipeline to enrich raw transactions.

Reads all transactions, computes enrichment fields, and populates the
transaction_enrichment table in batch chunks of 1000.

Enrichment fields computed:
  - amount_usd            : amount converted to USD using FX rates
  - is_round_amount       : 1 if amount is a "round" number
  - is_large_cash         : 1 if cash txn >= 10,000 USD
  - velocity_1h           : count of txns on same account in preceding 60 min
  - velocity_24h          : count of txns on same account in preceding 24 h
  - velocity_7d           : count of txns on same account in preceding 7 days
  - amount_velocity_24h   : total USD amount on account in preceding 24 h
  - country_risk_score    : risk_level from country_risk for counterparty country
  - is_new_counterparty   : 1 if this counterparty_account not seen before on this account
  - account_age_days      : days between account opened_date and transaction timestamp
"""

from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator

import db

# ─────────────────────────────────────────────────────────────────────────────
# FX rates (approximate 2024 averages, all vs USD)
# ─────────────────────────────────────────────────────────────────────────────
FX_RATES: dict[str, float] = {
    'USD': 1.0,
    'EUR': 1.09,
    'GBP': 1.27,
    'CAD': 0.74,
    'AUD': 0.66,
    'JPY': 0.0067,
    'CHF': 1.12,
    'MXN': 0.059,
    'BRL': 0.20,
    'INR': 0.012,
}

BATCH_SIZE = 1000
CASH_TYPES = {'cash_deposit', 'cash_withdrawal'}
LARGE_CASH_THRESHOLD_USD = 10_000.0


def to_usd(amount: float, currency: str) -> float:
    """Convert amount from `currency` to USD.

    Args:
        amount:   Transaction amount in original currency.
        currency: ISO 4217 three-letter currency code.

    Returns:
        Amount in USD, rounded to 2 decimal places.
        Falls back to 1:1 conversion if currency not in FX_RATES.
    """
    rate = FX_RATES.get(currency, 1.0)
    return round(amount * rate, 2)


def is_round_amount(amount: float, currency: str) -> bool:
    """Determine if an amount is psychologically "round".

    Heuristic: amount in USD is divisible by 100 with no cents, or
    the original amount ends in .00 or .50 and is >= 1000.

    Args:
        amount:   Transaction amount in original currency.
        currency: ISO 4217 currency code.

    Returns:
        True if the amount is considered round.
    """
    usd = to_usd(amount, currency)
    # Exact round thousands
    if usd >= 1_000 and usd % 1_000 == 0:
        return True
    # Round hundreds
    if usd >= 500 and usd % 100 == 0:
        return True
    # Round 50s for larger amounts
    if usd >= 5_000 and usd % 50 == 0:
        return True
    return False


def batch_iter(items: list, size: int = BATCH_SIZE) -> Iterator[list]:
    """Yield successive slices of `items` of length `size`.

    Args:
        items: List to batch.
        size:  Batch size.

    Yields:
        Successive sub-lists.
    """
    for i in range(0, len(items), size):
        yield items[i: i + size]


# ─────────────────────────────────────────────────────────────────────────────
# Core ETL
# ─────────────────────────────────────────────────────────────────────────────

def run_etl(conn: sqlite3.Connection, verbose: bool = True) -> int:
    """Compute and insert enrichment rows for all un-enriched transactions.

    Processes in batches of BATCH_SIZE.  For velocity calculations, uses
    a pre-built in-memory index (account_id -> sorted list of timestamps)
    to avoid N+1 query patterns.

    Args:
        conn:    Open SQLite connection (row_factory=sqlite3.Row).
        verbose: Whether to print progress.

    Returns:
        Number of enrichment rows inserted.
    """
    # ── Load reference data ───────────────────────────────────────────────
    country_risk_map: dict[str, int] = {
        row['country_code']: row['risk_level']
        for row in conn.execute("SELECT country_code, risk_level FROM country_risk").fetchall()
    }

    account_map: dict[int, dict] = {
        row['id']: dict(row)
        for row in conn.execute("SELECT id, currency, opened_date FROM accounts").fetchall()
    }

    # ── Load all transactions (sorted by account + time for velocity) ─────
    if verbose:
        print("[etl] Loading all transactions...")
    all_txns = conn.execute("""
        SELECT id, account_id, transaction_type, amount, currency,
               timestamp, counterparty_account, counterparty_country
        FROM transactions
        ORDER BY account_id, timestamp
    """).fetchall()

    if verbose:
        print(f"[etl] {len(all_txns):,} transactions loaded.")

    # ── Find already-enriched IDs ─────────────────────────────────────────
    enriched_ids: set[int] = {
        row[0] for row in conn.execute("SELECT transaction_id FROM transaction_enrichment").fetchall()
    }
    to_enrich = [t for t in all_txns if t['id'] not in enriched_ids]
    if verbose:
        print(f"[etl] {len(to_enrich):,} transactions need enrichment "
              f"({len(enriched_ids):,} already done).")

    if not to_enrich:
        return 0

    # ── Build account timeline index ──────────────────────────────────────
    # account_id -> list of (datetime, txn_id) sorted chronologically
    from collections import defaultdict
    account_timeline: dict[int, list[tuple[datetime, int]]] = defaultdict(list)
    for txn in all_txns:
        dt = datetime.fromisoformat(txn['timestamp'])
        account_timeline[txn['account_id']].append((dt, txn['id']))
    # Each sub-list is already sorted (we loaded ORDER BY account_id, timestamp)

    # ── Build counterparty history per account ────────────────────────────
    # account_id -> set of counterparty_account strings seen before
    seen_counterparties: dict[int, set[str]] = defaultdict(set)

    # ── Process in sorted order ───────────────────────────────────────────
    # Sort to_enrich by account_id + timestamp to compute velocity correctly
    to_enrich_sorted = sorted(
        to_enrich,
        key=lambda t: (t['account_id'], t['timestamp'])
    )

    enrichment_rows: list[tuple] = []
    processed = 0

    # Pointer index: for each account, track where we are in its timeline
    # We walk the timeline in order, so velocity lookups are O(log n) per txn
    import bisect

    for txn in to_enrich_sorted:
        tid      = txn['id']
        aid      = txn['account_id']
        amount   = txn['amount']
        currency = txn['currency']
        ts_str   = txn['timestamp']
        cp_acct  = txn['counterparty_account']
        cp_country = txn['counterparty_country']
        txn_type = txn['transaction_type']

        dt = datetime.fromisoformat(ts_str)
        timeline = account_timeline[aid]   # list of (datetime, id)
        timestamps_only = [t[0] for t in timeline]

        # ── USD conversion ────────────────────────────────────────────────
        amount_usd = to_usd(amount, currency)

        # ── Round amount ──────────────────────────────────────────────────
        round_flag = 1 if is_round_amount(amount, currency) else 0

        # ── Large cash ────────────────────────────────────────────────────
        large_cash_flag = 1 if (txn_type in CASH_TYPES and amount_usd >= LARGE_CASH_THRESHOLD_USD) else 0

        # ── Velocity (counts of txns in preceding windows) ────────────────
        # Current txn position in sorted timeline
        pos = bisect.bisect_left(timestamps_only, dt)

        # 1 hour window
        cutoff_1h  = dt - timedelta(hours=1)
        lo_1h  = bisect.bisect_left(timestamps_only, cutoff_1h)
        vel_1h = pos - lo_1h   # exclusive of current txn

        # 24 hour window
        cutoff_24h = dt - timedelta(hours=24)
        lo_24h = bisect.bisect_left(timestamps_only, cutoff_24h)
        vel_24h = pos - lo_24h

        # 7 day window
        cutoff_7d  = dt - timedelta(days=7)
        lo_7d  = bisect.bisect_left(timestamps_only, cutoff_7d)
        vel_7d = pos - lo_7d

        # ── Amount velocity (USD sum last 24h) ────────────────────────────
        # We need the actual amounts — stored in a parallel list
        # Build this on demand per account (lazy, first access)
        # Store in a shadow dict to avoid rebuilding
        if not hasattr(run_etl, '_amount_cache'):
            run_etl._amount_cache = {}  # type: ignore[attr-defined]
        if aid not in run_etl._amount_cache:  # type: ignore[attr-defined]
            # Build sorted (datetime, amount_usd) list for this account
            acct_rows = [t for t in all_txns if t['account_id'] == aid]
            run_etl._amount_cache[aid] = [  # type: ignore[attr-defined]
                (datetime.fromisoformat(t['timestamp']),
                 to_usd(t['amount'], t['currency']))
                for t in acct_rows
            ]
        amt_timeline = run_etl._amount_cache[aid]  # type: ignore[attr-defined]
        amt_ts_only  = [x[0] for x in amt_timeline]
        lo_24h_amt   = bisect.bisect_left(amt_ts_only, cutoff_24h)
        cur_pos_amt  = bisect.bisect_left(amt_ts_only, dt)
        amount_velocity_24h = round(
            sum(x[1] for x in amt_timeline[lo_24h_amt:cur_pos_amt]), 2
        )

        # ── Country risk score ────────────────────────────────────────────
        country_risk = country_risk_map.get(cp_country or '', 1)

        # ── New counterparty ──────────────────────────────────────────────
        if cp_acct:
            is_new_cp = 1 if cp_acct not in seen_counterparties[aid] else 0
            seen_counterparties[aid].add(cp_acct)
        else:
            is_new_cp = 0

        # ── Account age ───────────────────────────────────────────────────
        acct_info   = account_map[aid]
        opened_dt   = datetime.fromisoformat(acct_info['opened_date'])
        acct_age    = max(0, (dt.date() - opened_dt.date()).days)

        enrichment_rows.append((
            tid,
            amount_usd,
            round_flag,
            large_cash_flag,
            vel_1h,
            vel_24h,
            vel_7d,
            amount_velocity_24h,
            country_risk,
            is_new_cp,
            acct_age,
        ))
        processed += 1

    # ── Batch insert ─────────────────────────────────────────────────────
    if verbose:
        print(f"[etl] Inserting {len(enrichment_rows):,} enrichment rows...")

    total_inserted = 0
    for i, batch in enumerate(batch_iter(enrichment_rows, BATCH_SIZE)):
        conn.executemany("""
            INSERT OR REPLACE INTO transaction_enrichment
              (transaction_id, amount_usd, is_round_amount, is_large_cash,
               velocity_1h, velocity_24h, velocity_7d, amount_velocity_24h,
               country_risk_score, is_new_counterparty, account_age_days)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch)
        conn.commit()
        total_inserted += len(batch)
        if verbose and (i + 1) % 20 == 0:
            pct = 100 * total_inserted / len(enrichment_rows)
            print(f"  ... {total_inserted:,} / {len(enrichment_rows):,} ({pct:.0f}%)")

    return total_inserted


def spot_check(conn: sqlite3.Connection) -> None:
    """Run spot-check queries to verify enrichment quality.

    Args:
        conn: Open SQLite connection.
    """
    print("\n" + "=" * 60)
    print("ENRICHMENT SPOT CHECKS")
    print("=" * 60)

    # Coverage
    (total,) = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()
    (enriched,) = conn.execute("SELECT COUNT(*) FROM transaction_enrichment").fetchone()
    pct = 100 * enriched / total if total else 0
    print(f"\nCoverage: {enriched:,} / {total:,} transactions enriched ({pct:.1f}%)")

    # Large cash detections
    (large_cash,) = conn.execute(
        "SELECT COUNT(*) FROM transaction_enrichment WHERE is_large_cash = 1"
    ).fetchone()
    print(f"\nLarge cash transactions (>=10k USD): {large_cash:,}")

    # Verify large cash only on cash-type transactions
    bad = conn.execute("""
        SELECT COUNT(*) FROM transaction_enrichment te
        JOIN transactions t ON t.id = te.transaction_id
        WHERE te.is_large_cash = 1
          AND t.transaction_type NOT IN ('cash_deposit', 'cash_withdrawal')
    """).fetchone()[0]
    print(f"  Incorrectly flagged non-cash as large cash: {bad} (expect 0)")

    # Round amount stats
    (round_amt,) = conn.execute(
        "SELECT COUNT(*) FROM transaction_enrichment WHERE is_round_amount = 1"
    ).fetchone()
    print(f"\nRound amount transactions: {round_amt:,} ({100*round_amt/enriched:.1f}%)")

    # Velocity distribution
    rows = conn.execute("""
        SELECT
            MAX(velocity_1h)  max_1h,
            MAX(velocity_24h) max_24h,
            MAX(velocity_7d)  max_7d,
            AVG(velocity_1h)  avg_1h,
            AVG(velocity_24h) avg_24h,
            AVG(velocity_7d)  avg_7d
        FROM transaction_enrichment
    """).fetchone()
    print(f"\nVelocity stats:")
    print(f"  1h:  avg={rows['avg_1h']:.2f}  max={rows['max_1h']}")
    print(f"  24h: avg={rows['avg_24h']:.2f}  max={rows['max_24h']}")
    print(f"  7d:  avg={rows['avg_7d']:.2f}   max={rows['max_7d']}")

    # High velocity — should hit fraud accounts
    high_vel = conn.execute("""
        SELECT t.account_id, MAX(te.velocity_1h) max_vel_1h
        FROM transaction_enrichment te
        JOIN transactions t ON t.id = te.transaction_id
        GROUP BY t.account_id
        HAVING max_vel_1h >= 15
        ORDER BY max_vel_1h DESC
        LIMIT 10
    """).fetchall()
    print(f"\nTop high-velocity accounts (>=15 txns/hour):")
    for r in high_vel:
        print(f"  account_id={r['account_id']:>5}  max_vel_1h={r['max_vel_1h']}")

    # Structuring detection spot check
    struct = conn.execute("""
        SELECT COUNT(*) FROM transactions t
        JOIN transaction_enrichment te ON te.transaction_id = t.id
        WHERE t.transaction_type = 'cash_deposit'
          AND t.amount BETWEEN 9000 AND 9999
    """).fetchone()[0]
    print(f"\nCash deposits in $9000-$9999 range (structuring): {struct:,}")

    # Country risk distribution
    rows = conn.execute("""
        SELECT country_risk_score, COUNT(*) n
        FROM transaction_enrichment
        WHERE country_risk_score > 1
        GROUP BY country_risk_score
        ORDER BY country_risk_score
    """).fetchall()
    print(f"\nHigh-risk country transactions (enriched):")
    labels = {2: 'Medium', 3: 'High', 4: 'Very High', 5: 'Sanctioned'}
    for r in rows:
        print(f"  Risk {r['country_risk_score']} ({labels.get(r['country_risk_score'],'?'):>10}): {r['n']:,}")

    # New counterparty rate
    (new_cp,) = conn.execute(
        "SELECT COUNT(*) FROM transaction_enrichment WHERE is_new_counterparty = 1"
    ).fetchone()
    print(f"\nNew counterparty transactions: {new_cp:,} ({100*new_cp/enriched:.1f}%)")

    # USD conversion spot check
    rows = conn.execute("""
        SELECT t.currency, t.amount, te.amount_usd,
               ROUND(te.amount_usd / t.amount, 4) implied_rate
        FROM transactions t
        JOIN transaction_enrichment te ON te.transaction_id = t.id
        WHERE t.currency != 'USD'
        LIMIT 6
    """).fetchall()
    print(f"\nFX conversion spot check (non-USD):")
    print(f"  {'Currency':<10} {'Amount':>12} {'USD':>12} {'Rate':>8}")
    for r in rows:
        print(f"  {r['currency']:<10} {r['amount']:>12,.2f} {r['amount_usd']:>12,.2f} {r['implied_rate']:>8.4f}")

    # Account age stats
    rows = conn.execute("""
        SELECT MIN(account_age_days) min_age, AVG(account_age_days) avg_age,
               MAX(account_age_days) max_age
        FROM transaction_enrichment
    """).fetchone()
    print(f"\nAccount age at transaction (days): min={rows[0]}  avg={rows[1]:.0f}  max={rows[2]}")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    """Run the ETL pipeline end-to-end."""
    print("=" * 60)
    print("Transaction Monitor — ETL Pipeline")
    print("=" * 60)

    conn = db.get_connection()
    try:
        n = run_etl(conn, verbose=True)
        print(f"\n[etl] Complete. {n:,} enrichment rows inserted.")

        # Log to audit
        conn.execute("""
            INSERT INTO audit_log (event_type, description, metadata)
            VALUES ('etl_run', 'ETL enrichment complete', ?)
        """, (json.dumps({"rows_inserted": n}),))
        conn.commit()

        spot_check(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
