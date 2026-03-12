"""
data_generator.py — Synthetic transaction data generator.

Produces:
  - 2000 customers
  - ~5000 accounts
  - 200,000+ transactions over 12 months (2024-01-01 to 2024-12-31)

Includes 8 distinct fraud patterns embedded in ~50+ flagged customers.
"""

from __future__ import annotations

import json
import random
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
from faker import Faker

import db

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
SEED = 42
random.seed(SEED)
np.random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

START_DATE = datetime(2024, 1, 1)
END_DATE   = datetime(2024, 12, 31, 23, 59, 59)
TOTAL_DAYS = (END_DATE - START_DATE).days + 1

N_CUSTOMERS   = 2000
N_ACCOUNTS    = 5000
TARGET_TXNS   = 200_000

# Country pools weighted by realism (most customers from low-risk countries)
LOW_RISK_COUNTRIES    = ['US', 'GB', 'DE', 'FR', 'CA', 'AU', 'JP', 'SG', 'CH', 'NL', 'SE', 'NO']
MEDIUM_RISK_COUNTRIES = ['MX', 'BR', 'IN', 'ZA', 'AR', 'TR', 'AE', 'HK']
HIGH_RISK_COUNTRIES   = ['NG', 'PK', 'VN', 'KE', 'PH', 'TH', 'MA']
VERY_HIGH_COUNTRIES   = ['MM', 'AF', 'YE', 'LY', 'SY', 'VE']
SANCTIONED_COUNTRIES  = ['IR', 'KP', 'RU', 'CU']
ALL_COUNTRIES         = LOW_RISK_COUNTRIES + MEDIUM_RISK_COUNTRIES + HIGH_RISK_COUNTRIES

CURRENCIES = ['USD', 'EUR', 'GBP', 'CAD', 'AUD', 'JPY', 'CHF', 'MXN', 'BRL', 'INR']

# FX rates vs USD (approximate 2024)
FX_RATES = {
    'USD': 1.0, 'EUR': 1.09, 'GBP': 1.27, 'CAD': 0.74,
    'AUD': 0.66, 'JPY': 0.0067, 'CHF': 1.12, 'MXN': 0.059,
    'BRL': 0.20, 'INR': 0.012,
}

ACCOUNT_TYPES  = ['checking', 'savings', 'business', 'investment', 'crypto']
TXN_TYPES      = ['deposit', 'withdrawal', 'transfer_in', 'transfer_out',
                  'wire_in', 'wire_out', 'cash_deposit', 'cash_withdrawal']
CHANNELS       = ['online', 'mobile', 'branch', 'atm', 'wire', 'api']

# ─────────────────────────────────────────────────────────────────────────────
# Fraud pattern flags stored per customer
# Each key maps to a set of customer IDs
# ─────────────────────────────────────────────────────────────────────────────
FRAUD_CUSTOMERS: dict[str, set[int]] = {
    "structuring":        set(),  # pattern 1 – sub-$10k deposits
    "rapid_velocity":     set(),  # pattern 2 – 20+ txns/hour
    "round_trip":         set(),  # pattern 3 – out & back same counterparty
    "geo_risk":           set(),  # pattern 4 – sanctioned country activity
    "layering":           set(),  # pattern 5 – A->B->C->D chains
    "dormant_activation": set(),  # pattern 6 – dormant then high volume
    "unusual_amounts":    set(),  # pattern 7 – 50x normal
    "cross_acct_smurf":   set(),  # pattern 8 – sub-threshold multi-account
}


# ─────────────────────────────────────────────────────────────────────────────
# Time helpers
# ─────────────────────────────────────────────────────────────────────────────

def rand_datetime(start: datetime = START_DATE, end: datetime = END_DATE) -> datetime:
    """Return a uniformly random datetime in [start, end]."""
    delta = (end - start).total_seconds()
    return start + timedelta(seconds=random.random() * delta)


def weighted_hour() -> int:
    """Business-hours weighted hour (0–23). Peak 9am–5pm weekday pattern."""
    weights = [
        0.5, 0.3, 0.2, 0.2, 0.3, 0.5,   # 0–5
        1.0, 2.0, 3.5, 5.0, 5.5, 5.5,   # 6–11
        5.0, 5.0, 5.0, 4.5, 4.0, 3.5,   # 12–17
        3.0, 2.5, 2.0, 1.5, 1.0, 0.7,   # 18–23
    ]
    return random.choices(range(24), weights=weights)[0]


def seasonality_factor(dt: datetime) -> float:
    """Multiply base txn rate by a seasonal factor (higher in Dec, lower in Feb)."""
    # Monthly multipliers: Jan–Dec
    monthly = [0.85, 0.75, 0.90, 0.95, 1.00, 1.05,
               0.95, 1.00, 1.05, 1.10, 1.15, 1.35]
    return monthly[dt.month - 1]


# ─────────────────────────────────────────────────────────────────────────────
# Amount distributions
# ─────────────────────────────────────────────────────────────────────────────

def sample_amount(customer_type: str, txn_type: str) -> float:
    """Sample a realistic transaction amount using a power-law distribution.

    Business accounts have larger amounts; cash transactions tend to be smaller.
    """
    if 'cash' in txn_type:
        # Cash: mostly small, rarely large
        base = np.random.pareto(2.5) * 500 + 50
    elif customer_type == 'business':
        base = np.random.pareto(1.2) * 5000 + 500
    elif customer_type == 'financial_institution':
        base = np.random.pareto(0.8) * 50_000 + 10_000
    else:
        base = np.random.pareto(2.0) * 1000 + 25

    # Occasionally round to nice numbers (psychological rounding ~15% of txns)
    if random.random() < 0.15:
        magnitude = 10 ** max(1, int(np.log10(base)) - 1)
        base = round(base / magnitude) * magnitude

    return round(min(base, 10_000_000), 2)


# ─────────────────────────────────────────────────────────────────────────────
# Customer generation
# ─────────────────────────────────────────────────────────────────────────────

def generate_customers(n: int = N_CUSTOMERS) -> list[tuple]:
    """Generate n customer rows for bulk insert.

    Returns:
        List of tuples matching columns:
        (name, customer_type, risk_rating, country, registration_date, kyc_status, pep_status)
    """
    rows = []

    # Country weights: mostly low-risk
    all_countries = (
        LOW_RISK_COUNTRIES * 50 +
        MEDIUM_RISK_COUNTRIES * 15 +
        HIGH_RISK_COUNTRIES * 5
    )

    type_weights = {'individual': 0.65, 'business': 0.30, 'financial_institution': 0.05}
    types = list(type_weights.keys())
    type_probs = list(type_weights.values())

    for _ in range(n):
        ctype = random.choices(types, weights=type_probs)[0]

        if ctype == 'individual':
            name = fake.name()
        elif ctype == 'business':
            name = fake.company()
        else:
            name = fake.company() + " Financial"

        country = random.choice(all_countries)

        # Risk rating correlated with country risk and type
        if country in HIGH_RISK_COUNTRIES:
            risk = random.choices([1, 2, 3], weights=[0.1, 0.3, 0.6])[0]
        elif country in MEDIUM_RISK_COUNTRIES:
            risk = random.choices([1, 2, 3], weights=[0.3, 0.5, 0.2])[0]
        else:
            risk = random.choices([1, 2, 3], weights=[0.6, 0.3, 0.1])[0]

        # Registration date: 1–10 years before start
        reg_days_before = random.randint(30, 3650)
        reg_date = (START_DATE - timedelta(days=reg_days_before)).date().isoformat()

        kyc = random.choices(
            ['verified', 'pending', 'expired', 'failed'],
            weights=[0.80, 0.10, 0.07, 0.03]
        )[0]

        pep = 1 if (risk == 3 and random.random() < 0.15) else 0

        rows.append((name, ctype, risk, country, reg_date, kyc, pep))

    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Account generation
# ─────────────────────────────────────────────────────────────────────────────

def generate_accounts(customer_rows: list[sqlite3.Row]) -> list[tuple]:
    """Generate accounts for customers.

    Each customer gets 1–4 accounts with realistic type/currency mix.

    Returns:
        List of tuples: (customer_id, account_type, currency, opened_date, status, daily_limit)
    """
    rows = []
    for cust in customer_rows:
        cid   = cust['id']
        ctype = cust['customer_type']
        reg_d = datetime.fromisoformat(cust['registration_date'])

        if ctype == 'individual':
            n_accts = random.choices([1, 2, 3, 4], weights=[0.40, 0.35, 0.15, 0.10])[0]
            acct_pool = ['checking', 'savings', 'investment', 'crypto']
            acct_weights = [0.45, 0.30, 0.15, 0.10]
            currency_pool = ['USD', 'EUR', 'GBP', 'CAD', 'AUD']
            currency_weights = [0.60, 0.15, 0.10, 0.08, 0.07]
            limit_range = (500, 25_000)
        elif ctype == 'business':
            n_accts = random.choices([1, 2, 3, 4], weights=[0.20, 0.35, 0.30, 0.15])[0]
            acct_pool = ['checking', 'business', 'savings', 'crypto']
            acct_weights = [0.40, 0.40, 0.15, 0.05]
            currency_pool = ['USD', 'EUR', 'GBP', 'CAD', 'MXN', 'BRL']
            currency_weights = [0.50, 0.15, 0.10, 0.08, 0.10, 0.07]
            limit_range = (5_000, 500_000)
        else:  # financial_institution
            n_accts = random.choices([2, 3, 4], weights=[0.30, 0.40, 0.30])[0]
            acct_pool = ['business', 'checking', 'investment']
            acct_weights = [0.50, 0.30, 0.20]
            currency_pool = CURRENCIES
            currency_weights = [0.30, 0.20, 0.15, 0.08, 0.07, 0.06, 0.05, 0.04, 0.03, 0.02]
            limit_range = (100_000, 10_000_000)

        used_types = set()
        for _ in range(n_accts):
            atype = random.choices(acct_pool, weights=acct_weights)[0]
            # Avoid exact duplicates on type, allow repeats with small prob
            if atype in used_types and random.random() < 0.8:
                atype = random.choices(acct_pool, weights=acct_weights)[0]
            used_types.add(atype)

            currency = random.choices(currency_pool, weights=currency_weights)[0]

            # Opened date: after registration, before end
            days_after_reg = random.randint(0, max(0, (END_DATE.date() - reg_d.date()).days))
            opened = (reg_d + timedelta(days=days_after_reg)).date().isoformat()

            status = random.choices(
                ['active', 'frozen', 'closed'],
                weights=[0.90, 0.05, 0.05]
            )[0]

            daily_limit = round(
                random.uniform(*limit_range) / 100
            ) * 100  # rounded to nearest 100

            rows.append((cid, atype, currency, opened, status, daily_limit))

    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Normal transaction generation
# ─────────────────────────────────────────────────────────────────────────────

def _txn_row(
    account_id: int,
    acct: sqlite3.Row,
    cust: sqlite3.Row,
    dt: datetime,
    txn_type: Optional[str] = None,
    amount: Optional[float] = None,
    counterparty_country: Optional[str] = None,
    counterparty_account: Optional[str] = None,
    counterparty_name: Optional[str] = None,
    channel: Optional[str] = None,
) -> tuple:
    """Construct a single transaction row tuple."""
    if txn_type is None:
        txn_type = random.choices(
            TXN_TYPES,
            weights=[0.20, 0.15, 0.18, 0.17, 0.08, 0.07, 0.08, 0.07]
        )[0]

    if amount is None:
        amount = sample_amount(cust['customer_type'], txn_type)

    if channel is None:
        if 'cash' in txn_type:
            channel = random.choices(['branch', 'atm'], weights=[0.6, 0.4])[0]
        elif 'wire' in txn_type:
            channel = 'wire'
        else:
            channel = random.choices(
                ['online', 'mobile', 'branch', 'atm', 'api'],
                weights=[0.30, 0.40, 0.15, 0.10, 0.05]
            )[0]

    if counterparty_country is None and 'transfer' in txn_type or 'wire' in txn_type:
        counterparty_country = random.choices(
            ALL_COUNTRIES + LOW_RISK_COUNTRIES,
            k=1
        )[0]

    if counterparty_account is None and txn_type in ('transfer_out', 'transfer_in', 'wire_out', 'wire_in'):
        counterparty_account = fake.bothify('ACCT-########')
    if counterparty_name is None and counterparty_account:
        counterparty_name = fake.name() if random.random() < 0.7 else fake.company()

    return (
        account_id,
        txn_type,
        amount,
        acct['currency'],
        dt.isoformat(timespec='seconds'),
        counterparty_account,
        counterparty_name,
        counterparty_country,
        channel,
        'completed',
    )


def generate_normal_transactions(
    accounts: list[sqlite3.Row],
    customer_map: dict[int, sqlite3.Row],
    target: int = TARGET_TXNS,
) -> list[tuple]:
    """Generate baseline normal transactions across all active accounts.

    Args:
        accounts: All account rows.
        customer_map: Mapping customer_id -> customer row.
        target: Approximate number of transactions to generate.

    Returns:
        List of transaction tuples.
    """
    active_accounts = [a for a in accounts if a['status'] == 'active']
    txns: list[tuple] = []

    # Weight accounts by customer type: businesses transact more
    type_mult = {'individual': 1.0, 'business': 3.0, 'financial_institution': 8.0}
    weights = [
        type_mult.get(customer_map[a['customer_id']]['customer_type'], 1.0)
        for a in active_accounts
    ]
    total_weight = sum(weights)
    per_account = [int(target * w / total_weight) for w in weights]

    # Generate day-by-day for each account
    for acct, n_txns in zip(active_accounts, per_account):
        if n_txns == 0:
            n_txns = 1
        cust = customer_map[acct['customer_id']]
        opened = datetime.fromisoformat(acct['opened_date'])

        for _ in range(n_txns):
            # Random date after account opened
            min_start = max(START_DATE, opened)
            if min_start >= END_DATE:
                continue
            dt = rand_datetime(min_start, END_DATE)
            # Apply hourly seasonality
            dt = dt.replace(hour=weighted_hour(), minute=random.randint(0, 59))
            txns.append(_txn_row(acct['id'], acct, cust, dt))

    return txns


# ─────────────────────────────────────────────────────────────────────────────
# Fraud pattern injectors
# ─────────────────────────────────────────────────────────────────────────────

def inject_pattern_1_structuring(
    accounts: list[sqlite3.Row],
    customer_map: dict[int, sqlite3.Row],
    n_customers: int = 12,
) -> list[tuple]:
    """Pattern 1: Structuring / smurfing — repeated cash deposits $9000–$9999.

    Avoids the $10,000 CTR reporting threshold.
    """
    txns = []
    candidates = [a for a in accounts if a['status'] == 'active']
    chosen_accts = random.sample(candidates, min(n_customers, len(candidates)))

    for acct in chosen_accts:
        cid = acct['customer_id']
        FRAUD_CUSTOMERS["structuring"].add(cid)
        cust = customer_map[cid]
        # 8–25 structuring deposits spread over 30–90 day window
        n_deposits = random.randint(8, 25)
        window_start = rand_datetime(START_DATE, END_DATE - timedelta(days=90))
        for i in range(n_deposits):
            dt = window_start + timedelta(
                days=random.randint(0, 60),
                hours=random.randint(8, 17),
                minutes=random.randint(0, 59)
            )
            amount = round(random.uniform(9000, 9999), 2)
            txns.append(_txn_row(
                acct['id'], acct, cust, dt,
                txn_type='cash_deposit',
                amount=amount,
                channel='branch',
            ))
    print(f"  [P1] Structuring: {len(chosen_accts)} accounts, {len(txns)} txns")
    return txns


def inject_pattern_2_rapid_velocity(
    accounts: list[sqlite3.Row],
    customer_map: dict[int, sqlite3.Row],
    n_customers: int = 8,
) -> list[tuple]:
    """Pattern 2: Rapid velocity — 20–50 transactions within a single hour."""
    txns = []
    candidates = [a for a in accounts if a['status'] == 'active']
    chosen_accts = random.sample(candidates, min(n_customers, len(candidates)))

    for acct in chosen_accts:
        cid = acct['customer_id']
        FRAUD_CUSTOMERS["rapid_velocity"].add(cid)
        cust = customer_map[cid]
        # 1–3 burst events per account
        n_bursts = random.randint(1, 3)
        for _ in range(n_bursts):
            burst_start = rand_datetime(
                START_DATE + timedelta(days=30),
                END_DATE - timedelta(hours=2)
            )
            n_txns = random.randint(20, 50)
            for j in range(n_txns):
                dt = burst_start + timedelta(minutes=j * (60 // n_txns), seconds=random.randint(0, 30))
                amount = round(random.uniform(100, 2000), 2)
                txns.append(_txn_row(
                    acct['id'], acct, cust, dt,
                    txn_type=random.choice(['transfer_out', 'withdrawal']),
                    amount=amount,
                    channel='api',
                ))
    print(f"  [P2] Rapid velocity: {len(chosen_accts)} accounts, {len(txns)} txns")
    return txns


def inject_pattern_3_round_trip(
    accounts: list[sqlite3.Row],
    customer_map: dict[int, sqlite3.Row],
    n_customers: int = 10,
) -> list[tuple]:
    """Pattern 3: Round-trip transfers — funds go out and return within 48 hours."""
    txns = []
    candidates = [a for a in accounts if a['status'] == 'active']
    chosen_accts = random.sample(candidates, min(n_customers, len(candidates)))

    for acct in chosen_accts:
        cid = acct['customer_id']
        FRAUD_CUSTOMERS["round_trip"].add(cid)
        cust = customer_map[cid]
        counterparty_acct = fake.bothify('RT-ACCT-########')
        counterparty_name = fake.company()
        n_trips = random.randint(3, 8)
        for _ in range(n_trips):
            out_dt = rand_datetime(
                START_DATE + timedelta(days=30),
                END_DATE - timedelta(days=3)
            )
            amount = round(random.uniform(10_000, 150_000), 2)
            # Out leg
            txns.append(_txn_row(
                acct['id'], acct, cust, out_dt,
                txn_type='wire_out',
                amount=amount,
                counterparty_account=counterparty_acct,
                counterparty_name=counterparty_name,
                counterparty_country=random.choice(MEDIUM_RISK_COUNTRIES),
                channel='wire',
            ))
            # Return leg: slightly smaller (fee deducted), within 6–36 hours
            return_delay = timedelta(hours=random.uniform(6, 36))
            return_amount = round(amount * random.uniform(0.97, 0.999), 2)
            txns.append(_txn_row(
                acct['id'], acct, cust, out_dt + return_delay,
                txn_type='wire_in',
                amount=return_amount,
                counterparty_account=counterparty_acct,
                counterparty_name=counterparty_name,
                counterparty_country=random.choice(MEDIUM_RISK_COUNTRIES),
                channel='wire',
            ))
    print(f"  [P3] Round-trip: {len(chosen_accts)} accounts, {len(txns)} txns")
    return txns


def inject_pattern_4_geo_risk(
    accounts: list[sqlite3.Row],
    customer_map: dict[int, sqlite3.Row],
    n_customers: int = 10,
) -> list[tuple]:
    """Pattern 4: Geographic risk — transfers to/from sanctioned/very-high-risk countries."""
    txns = []
    # Only use accounts from low-risk customers (makes the geo anomaly stand out)
    candidates = [
        a for a in accounts
        if a['status'] == 'active'
        and customer_map[a['customer_id']]['risk_rating'] == 1
    ]
    chosen_accts = random.sample(candidates, min(n_customers, len(candidates)))

    for acct in chosen_accts:
        cid = acct['customer_id']
        FRAUD_CUSTOMERS["geo_risk"].add(cid)
        cust = customer_map[cid]
        bad_countries = SANCTIONED_COUNTRIES + VERY_HIGH_COUNTRIES
        n_txns = random.randint(3, 10)
        for _ in range(n_txns):
            dt = rand_datetime(START_DATE + timedelta(days=60), END_DATE)
            amount = round(random.uniform(5_000, 100_000), 2)
            bad_country = random.choice(bad_countries)
            txns.append(_txn_row(
                acct['id'], acct, cust, dt,
                txn_type=random.choice(['wire_out', 'transfer_out']),
                amount=amount,
                counterparty_account=fake.bothify('INTL-########'),
                counterparty_name=fake.company(),
                counterparty_country=bad_country,
                channel='wire',
            ))
    print(f"  [P4] Geo risk: {len(chosen_accts)} accounts, {len(txns)} txns")
    return txns


def inject_pattern_5_layering(
    accounts: list[sqlite3.Row],
    customer_map: dict[int, sqlite3.Row],
    n_chains: int = 8,
) -> list[tuple]:
    """Pattern 5: Layering — A->B->C->D transfer chains within same day.

    Each chain involves 3–4 distinct accounts quickly passing funds.
    """
    txns = []
    candidates = [a for a in accounts if a['status'] == 'active']

    for chain_idx in range(n_chains):
        chain_length = random.randint(3, 4)
        chain_accounts = random.sample(candidates, chain_length)

        for acct in chain_accounts:
            FRAUD_CUSTOMERS["layering"].add(acct['customer_id'])

        chain_start = rand_datetime(
            START_DATE + timedelta(days=30),
            END_DATE - timedelta(hours=12)
        )
        amount = round(random.uniform(50_000, 500_000), 2)

        for i, acct in enumerate(chain_accounts):
            cust = customer_map[acct['customer_id']]
            dt = chain_start + timedelta(minutes=i * random.randint(15, 90))
            # Each hop: receives and immediately sends out (minus small fee)
            next_acct = chain_accounts[i + 1] if i < len(chain_accounts) - 1 else None
            if next_acct:
                # Outgoing leg of this hop
                txns.append(_txn_row(
                    acct['id'], acct, cust, dt,
                    txn_type='wire_out',
                    amount=round(amount * random.uniform(0.97, 0.999), 2),
                    counterparty_account=f'LAYER-{chain_idx:03d}-{i+1:02d}',
                    counterparty_name=customer_map[next_acct['customer_id']]['name'],
                    counterparty_country=random.choice(MEDIUM_RISK_COUNTRIES + HIGH_RISK_COUNTRIES),
                    channel='wire',
                ))
                # Incoming leg for next in chain
                txns.append(_txn_row(
                    next_acct['id'], next_acct, customer_map[next_acct['customer_id']],
                    dt + timedelta(minutes=random.randint(2, 15)),
                    txn_type='wire_in',
                    amount=round(amount * random.uniform(0.97, 0.999), 2),
                    counterparty_account=f'LAYER-{chain_idx:03d}-{i:02d}',
                    counterparty_name=cust['name'],
                    counterparty_country=random.choice(MEDIUM_RISK_COUNTRIES),
                    channel='wire',
                ))
            amount = round(amount * random.uniform(0.95, 0.998), 2)

    print(f"  [P5] Layering: {len(FRAUD_CUSTOMERS['layering'])} accounts, {len(txns)} txns")
    return txns


def inject_pattern_6_dormant_activation(
    accounts: list[sqlite3.Row],
    customer_map: dict[int, sqlite3.Row],
    n_customers: int = 8,
) -> list[tuple]:
    """Pattern 6: Dormant activation — account silent for 6+ months, then sudden surge."""
    txns = []
    candidates = [a for a in accounts if a['status'] == 'active']
    chosen_accts = random.sample(candidates, min(n_customers, len(candidates)))

    for acct in chosen_accts:
        cid = acct['customer_id']
        FRAUD_CUSTOMERS["dormant_activation"].add(cid)
        cust = customer_map[cid]
        # Dormant period: Jan–Jun 2024, activation: Jul–Dec 2024
        activation_date = datetime(2024, random.randint(7, 10), random.randint(1, 28))
        n_post_txns = random.randint(20, 60)
        for i in range(n_post_txns):
            dt = activation_date + timedelta(
                days=random.randint(0, 30),
                hours=random.randint(8, 20)
            )
            amount = round(random.uniform(5_000, 50_000), 2)
            txns.append(_txn_row(
                acct['id'], acct, cust, dt,
                txn_type=random.choice(['transfer_out', 'wire_out', 'withdrawal']),
                amount=amount,
                channel=random.choice(['online', 'wire']),
            ))
    print(f"  [P6] Dormant activation: {len(chosen_accts)} accounts, {len(txns)} txns")
    return txns


def inject_pattern_7_unusual_amounts(
    accounts: list[sqlite3.Row],
    customer_map: dict[int, sqlite3.Row],
    n_customers: int = 8,
) -> list[tuple]:
    """Pattern 7: Unusual amounts — transactions 50× the customer's normal level."""
    txns = []
    # Prefer individual accounts (anomaly more visible)
    candidates = [
        a for a in accounts
        if a['status'] == 'active'
        and customer_map[a['customer_id']]['customer_type'] == 'individual'
    ]
    chosen_accts = random.sample(candidates, min(n_customers, len(candidates)))

    for acct in chosen_accts:
        cid = acct['customer_id']
        FRAUD_CUSTOMERS["unusual_amounts"].add(cid)
        cust = customer_map[cid]
        n_txns = random.randint(3, 8)
        for _ in range(n_txns):
            dt = rand_datetime(START_DATE + timedelta(days=60), END_DATE)
            # Typical individual amount is ~$500–$2000; spike to $50k–$200k
            amount = round(random.uniform(50_000, 200_000), 2)
            txns.append(_txn_row(
                acct['id'], acct, cust, dt,
                txn_type=random.choice(['wire_out', 'transfer_out']),
                amount=amount,
                channel='wire',
            ))
    print(f"  [P7] Unusual amounts: {len(chosen_accts)} accounts, {len(txns)} txns")
    return txns


def inject_pattern_8_cross_account_smurfing(
    accounts: list[sqlite3.Row],
    customer_map: dict[int, sqlite3.Row],
    n_customers: int = 8,
) -> list[tuple]:
    """Pattern 8: Cross-account smurfing — sub-$10k deposits into multiple accounts same day.

    Same orchestrating customer uses several accounts to deposit just under $10k each.
    """
    txns = []
    # Find customers with 2+ accounts
    from collections import defaultdict
    cust_accounts: dict[int, list] = defaultdict(list)
    for a in accounts:
        if a['status'] == 'active':
            cust_accounts[a['customer_id']].append(a)

    multi_acct_customers = [cid for cid, accts in cust_accounts.items() if len(accts) >= 2]
    chosen_custs = random.sample(multi_acct_customers, min(n_customers, len(multi_acct_customers)))

    for cid in chosen_custs:
        FRAUD_CUSTOMERS["cross_acct_smurf"].add(cid)
        cust = customer_map[cid]
        acct_list = cust_accounts[cid]
        n_days = random.randint(5, 15)
        for _ in range(n_days):
            day_start = rand_datetime(START_DATE + timedelta(days=30), END_DATE - timedelta(days=1))
            day_start = day_start.replace(hour=0, minute=0, second=0)
            for acct in acct_list:
                deposit_hour = random.randint(8, 17)
                dt = day_start + timedelta(
                    hours=deposit_hour,
                    minutes=random.randint(0, 59)
                )
                amount = round(random.uniform(8_500, 9_999), 2)
                txns.append(_txn_row(
                    acct['id'], acct, cust, dt,
                    txn_type='cash_deposit',
                    amount=amount,
                    channel='branch',
                ))

    print(f"  [P8] Cross-account smurfing: {len(chosen_custs)} accounts, {len(txns)} txns")
    return txns


# ─────────────────────────────────────────────────────────────────────────────
# Stats printing
# ─────────────────────────────────────────────────────────────────────────────

def print_distribution_stats(conn: sqlite3.Connection) -> None:
    """Query and print key distribution stats from the database."""
    print("\n" + "=" * 60)
    print("DISTRIBUTION STATS")
    print("=" * 60)

    # Transactions by type
    rows = conn.execute("""
        SELECT transaction_type, COUNT(*) n,
               ROUND(AVG(amount),2) avg_amt,
               ROUND(MIN(amount),2) min_amt,
               ROUND(MAX(amount),2) max_amt
        FROM transactions
        GROUP BY transaction_type
        ORDER BY n DESC
    """).fetchall()
    print("\nTransactions by type:")
    print(f"  {'Type':<20} {'Count':>8} {'Avg $':>12} {'Min $':>12} {'Max $':>12}")
    for r in rows:
        print(f"  {r['transaction_type']:<20} {r['n']:>8,} {r['avg_amt']:>12,.2f} {r['min_amt']:>12,.2f} {r['max_amt']:>12,.2f}")

    # Monthly transaction volumes
    rows = conn.execute("""
        SELECT strftime('%Y-%m', timestamp) month,
               COUNT(*) n,
               ROUND(SUM(amount),0) total_vol
        FROM transactions
        GROUP BY month
        ORDER BY month
    """).fetchall()
    print("\nMonthly transaction volume:")
    for r in rows:
        bar = '#' * (r['n'] // 1000)
        print(f"  {r['month']}: {r['n']:>7,} txns  ${r['total_vol']:>15,.0f}  {bar}")

    # Channel distribution
    rows = conn.execute("""
        SELECT channel, COUNT(*) n, ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(),1) pct
        FROM transactions GROUP BY channel ORDER BY n DESC
    """).fetchall()
    print("\nChannel distribution:")
    for r in rows:
        print(f"  {r['channel']:<10}: {r['n']:>8,}  ({r['pct']}%)")

    # Customer risk distribution
    rows = conn.execute("""
        SELECT c.risk_rating, COUNT(DISTINCT c.id) n_cust,
               COUNT(t.id) n_txns
        FROM customers c
        LEFT JOIN accounts a ON a.customer_id = c.id
        LEFT JOIN transactions t ON t.account_id = a.id
        GROUP BY c.risk_rating
    """).fetchall()
    labels = {1: 'Low', 2: 'Medium', 3: 'High'}
    print("\nCustomer risk rating summary:")
    for r in rows:
        print(f"  Risk {r['risk_rating']} ({labels[r['risk_rating']]:>6}): {r['n_cust']:>5} customers, {r['n_txns']:>8,} txns")

    # Fraud pattern coverage
    print("\nFraud pattern customer coverage:")
    total_fraud = len(set().union(*FRAUD_CUSTOMERS.values()))
    for pattern, cids in FRAUD_CUSTOMERS.items():
        print(f"  {pattern:<25}: {len(cids):>4} customers")
    print(f"  {'TOTAL UNIQUE FRAUD CUSTS':<25}: {total_fraud:>4}")

    # Amount percentiles
    rows = conn.execute("""
        SELECT
            ROUND(MIN(amount),2) p0,
            ROUND(AVG(CASE WHEN pct <= 0.25 THEN amount END),2) p25,
            ROUND(AVG(CASE WHEN pct <= 0.50 THEN amount END),2) p50,
            ROUND(AVG(CASE WHEN pct <= 0.75 THEN amount END),2) p75,
            ROUND(AVG(CASE WHEN pct <= 0.95 THEN amount END),2) p95,
            ROUND(MAX(amount),2) p100
        FROM (
            SELECT amount, PERCENT_RANK() OVER (ORDER BY amount) pct
            FROM transactions
        )
    """).fetchone()
    print(f"\nAmount percentiles (USD-equivalent):")
    print(f"  p0={rows[0]:,.2f}  p25={rows[1]:,.2f}  p50={rows[2]:,.2f}  p75={rows[3]:,.2f}  p95={rows[4]:,.2f}  p100={rows[5]:,.2f}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    """Entry point: generate all data and insert into the database."""
    print("=" * 60)
    print("Transaction Monitor — Data Generator")
    print("=" * 60)

    # Fresh DB
    if db.DB_PATH.exists():
        db.DB_PATH.unlink()
        print(f"[gen] Removed existing DB: {db.DB_PATH}")
    db.init_db()

    conn = db.get_connection()
    try:
        # ── Customers ────────────────────────────────────────────────────────
        print(f"\n[gen] Generating {N_CUSTOMERS} customers...")
        cust_rows = generate_customers(N_CUSTOMERS)
        conn.executemany("""
            INSERT INTO customers
              (name, customer_type, risk_rating, country, registration_date, kyc_status, pep_status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, cust_rows)
        conn.commit()
        customers = conn.execute("SELECT * FROM customers").fetchall()
        customer_map: dict[int, sqlite3.Row] = {c['id']: c for c in customers}
        print(f"  Inserted {len(customers)} customers")

        # ── Accounts ─────────────────────────────────────────────────────────
        print(f"\n[gen] Generating accounts...")
        acct_rows = generate_accounts(customers)
        conn.executemany("""
            INSERT INTO accounts
              (customer_id, account_type, currency, opened_date, status, daily_limit)
            VALUES (?, ?, ?, ?, ?, ?)
        """, acct_rows)
        conn.commit()
        accounts = conn.execute("SELECT * FROM accounts").fetchall()
        print(f"  Inserted {len(accounts)} accounts")

        # ── Normal transactions ───────────────────────────────────────────────
        print(f"\n[gen] Generating ~{TARGET_TXNS:,} baseline transactions...")
        normal_txns = generate_normal_transactions(accounts, customer_map, TARGET_TXNS)
        print(f"  Generated {len(normal_txns):,} normal transactions, inserting...")

        def insert_batch(txns: list[tuple]) -> None:
            conn.executemany("""
                INSERT INTO transactions
                  (account_id, transaction_type, amount, currency, timestamp,
                   counterparty_account, counterparty_name, counterparty_country,
                   channel, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, txns)
            conn.commit()

        insert_batch(normal_txns)

        # ── Fraud patterns ────────────────────────────────────────────────────
        print("\n[gen] Injecting fraud patterns...")
        fraud_txns: list[tuple] = []
        fraud_txns += inject_pattern_1_structuring(accounts, customer_map)
        fraud_txns += inject_pattern_2_rapid_velocity(accounts, customer_map)
        fraud_txns += inject_pattern_3_round_trip(accounts, customer_map)
        fraud_txns += inject_pattern_4_geo_risk(accounts, customer_map)
        fraud_txns += inject_pattern_5_layering(accounts, customer_map)
        fraud_txns += inject_pattern_6_dormant_activation(accounts, customer_map)
        fraud_txns += inject_pattern_7_unusual_amounts(accounts, customer_map)
        fraud_txns += inject_pattern_8_cross_account_smurfing(accounts, customer_map)

        print(f"\n  Total fraud transactions: {len(fraud_txns):,}")
        insert_batch(fraud_txns)

        # ── Summary ───────────────────────────────────────────────────────────
        (total_txns,) = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()
        print(f"\n[gen] Total transactions inserted: {total_txns:,}")

        # Verify fraud customer count
        all_fraud_cids = set().union(*FRAUD_CUSTOMERS.values())
        print(f"[gen] Unique fraud customers: {len(all_fraud_cids)}")

        # Save fraud label metadata to audit log
        fraud_metadata = {
            pattern: list(cids)
            for pattern, cids in FRAUD_CUSTOMERS.items()
        }
        conn.execute("""
            INSERT INTO audit_log (event_type, description, metadata)
            VALUES ('data_loaded', 'Synthetic data generation complete', ?)
        """, (json.dumps({
            "n_customers": len(customers),
            "n_accounts": len(accounts),
            "n_transactions": total_txns,
            "fraud_customers": {k: list(v) for k, v in FRAUD_CUSTOMERS.items()},
        }),))
        conn.commit()

        # ── Distribution stats ─────────────────────────────────────────────────
        print_distribution_stats(conn)

    finally:
        conn.close()

    print("\n[gen] Done.")


if __name__ == "__main__":
    main()
