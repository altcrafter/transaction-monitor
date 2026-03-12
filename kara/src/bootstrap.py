"""
bootstrap.py - Generate a realistic AML/fraud-detection SQLite database.

Tables
------
customers       500 rows
accounts        800 rows
transactions    50,000 rows  (12 months ending 2026-03-12)
rules            15 rows
alerts         2,000 rows
country_risk    30+ rows

Run
---
    python src/bootstrap.py

Requires: faker, numpy  (pip install faker numpy)
"""

import sqlite3
import random
import datetime
from pathlib import Path

import numpy as np
from faker import Faker

# ---------------------------------------------------------------------------
# Reproducibility
# ---------------------------------------------------------------------------
SEED = 42
random.seed(SEED)
np.random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR / "data" / "transactions.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------
END_DATE   = datetime.date(2026, 3, 12)
START_DATE = END_DATE - datetime.timedelta(days=365)

NATIONALITIES = [
    "US", "GB", "DE", "FR", "CN", "RU", "BR", "IN", "MX", "NG",
    "ZA", "AE", "SG", "JP", "CA", "AU", "CH", "NL", "IT", "ES",
    "PK", "IR", "KP", "VE", "AF", "MM", "BY", "CU", "LY", "SD",
]

HIGH_RISK_COUNTRIES = {"IR", "KP", "VE", "AF", "MM", "BY", "CU", "LY", "SD", "RU", "NG"}
MEDIUM_RISK_COUNTRIES = {"CN", "MX", "PK", "ZA", "BR", "AE", "EG", "UA", "KZ", "TH"}

CURRENCIES = ["USD", "EUR", "GBP", "CHF", "JPY", "AED", "CNY", "BRL", "RUB", "NGN"]

KYC_STATUSES  = ["verified", "pending", "expired", "failed"]
RISK_RATINGS  = ["low", "medium", "high", "critical"]
ACCOUNT_TYPES = ["checking", "savings", "business"]
TXN_TYPES     = ["credit", "debit", "transfer", "wire"]
TXN_STATUSES  = ["completed", "pending", "failed", "reversed"]
CHANNELS      = ["online", "branch", "mobile", "atm"]
ACCT_STATUSES = ["active", "closed", "frozen"]

ANALYST_NAMES = [
    "Alice Morgan", "Bob Chen", "Carol Davis", "David Kim",
    "Eva Patel",   "Frank Wu",  "Grace Lopez",  "Henry Osei",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def random_date(start: datetime.date, end: datetime.date) -> datetime.date:
    delta = (end - start).days
    return start + datetime.timedelta(days=random.randint(0, delta))


def random_datetime(start: datetime.date, end: datetime.date) -> datetime.datetime:
    d = random_date(start, end)
    t = datetime.time(random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
    return datetime.datetime.combine(d, t)


def ts(dt) -> str:
    """Return ISO-8601 string from date or datetime."""
    if isinstance(dt, datetime.datetime):
        return dt.isoformat(sep=" ")
    return datetime.datetime.combine(dt, datetime.time()).isoformat(sep=" ")


def weighted_choice(choices, weights):
    return random.choices(choices, weights=weights, k=1)[0]


def realistic_amount() -> float:
    """
    Realistic transaction amount distribution:
      70 %  small    10 – 4,999
      20 %  medium   5,000 – 49,999
       8 %  large    50,000 – 499,999
       2 %  very     500,000 – 5,000,000
    """
    tier = random.random()
    if tier < 0.70:
        return round(random.uniform(10, 4999), 2)
    elif tier < 0.90:
        return round(random.uniform(5000, 49999), 2)
    elif tier < 0.98:
        return round(random.uniform(50000, 499999), 2)
    else:
        return round(random.uniform(500000, 5_000_000), 2)


# ---------------------------------------------------------------------------
# country_risk
# ---------------------------------------------------------------------------
COUNTRY_RISK_DATA = [
    ("US", "United States",     "low",      "member",       "FATF founding member"),
    ("GB", "United Kingdom",    "low",      "member",       "FATF founding member"),
    ("DE", "Germany",           "low",      "member",       "Strong AML framework"),
    ("FR", "France",            "low",      "member",       "FATF founding member"),
    ("CH", "Switzerland",       "low",      "member",       "Robust financial regulation"),
    ("NL", "Netherlands",       "low",      "member",       "EU AML directives applied"),
    ("IT", "Italy",             "low",      "member",       "EU member"),
    ("ES", "Spain",             "low",      "member",       "EU member"),
    ("CA", "Canada",            "low",      "member",       "FATF member"),
    ("AU", "Australia",         "low",      "member",       "FATF member"),
    ("JP", "Japan",             "low",      "member",       "FATF member"),
    ("SG", "Singapore",         "low",      "member",       "Regional AML hub"),
    ("IN", "India",             "medium",   "member",       "Improving AML controls"),
    ("CN", "China",             "medium",   "member",       "Monitored for compliance"),
    ("MX", "Mexico",            "medium",   "member",       "Drug-related ML concerns"),
    ("BR", "Brazil",            "medium",   "member",       "Emerging market risks"),
    ("ZA", "South Africa",      "medium",   "grey_list",    "FATF grey-listed 2023"),
    ("AE", "United Arab Emirates","medium", "grey_list",    "FATF grey-listed"),
    ("PK", "Pakistan",          "medium",   "grey_list",    "Enhanced monitoring"),
    ("EG", "Egypt",             "medium",   "grey_list",    "Grey-listed"),
    ("UA", "Ukraine",           "medium",   "member",       "Conflict-related risks"),
    ("KZ", "Kazakhstan",        "medium",   "member",       "Former Soviet state risks"),
    ("TH", "Thailand",          "medium",   "grey_list",    "Trafficking concerns"),
    ("NG", "Nigeria",           "high",     "grey_list",    "High fraud & corruption"),
    ("RU", "Russia",            "high",     "suspended",    "Sanctions & FATF suspended"),
    ("MM", "Myanmar",           "high",     "black_list",   "FATF black-listed"),
    ("AF", "Afghanistan",       "high",     "black_list",   "FATF black-listed"),
    ("KP", "North Korea",       "critical", "black_list",   "UN sanctions, FATF black-listed"),
    ("IR", "Iran",              "critical", "black_list",   "UN sanctions, FATF black-listed"),
    ("VE", "Venezuela",         "critical", "grey_list",    "Severe corruption & sanctions"),
    ("BY", "Belarus",           "high",     "suspended",    "EU & US sanctions"),
    ("CU", "Cuba",              "high",     "grey_list",    "US sanctions"),
    ("LY", "Libya",             "high",     "grey_list",    "Political instability"),
    ("SD", "Sudan",             "high",     "grey_list",    "US sanctions history"),
]


# ---------------------------------------------------------------------------
# Build database
# ---------------------------------------------------------------------------

def build_db():
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    # ------------------------------------------------------------------ DDL
    cur.executescript("""
    PRAGMA journal_mode=WAL;
    PRAGMA foreign_keys=ON;

    CREATE TABLE country_risk (
        country_code   TEXT PRIMARY KEY,
        country_name   TEXT NOT NULL,
        risk_level     TEXT NOT NULL,
        fatf_status    TEXT NOT NULL,
        comments       TEXT
    );

    CREATE TABLE customers (
        customer_id          TEXT PRIMARY KEY,
        name                 TEXT NOT NULL,
        email                TEXT NOT NULL,
        date_of_birth        TEXT NOT NULL,
        nationality          TEXT NOT NULL,
        kyc_status           TEXT NOT NULL,
        risk_rating          TEXT NOT NULL,
        pep_flag             INTEGER NOT NULL DEFAULT 0,
        sanctions_flag       INTEGER NOT NULL DEFAULT 0,
        created_at           TEXT NOT NULL,
        country_of_residence TEXT NOT NULL
    );

    CREATE TABLE accounts (
        account_id    TEXT PRIMARY KEY,
        customer_id   TEXT NOT NULL REFERENCES customers(customer_id),
        account_type  TEXT NOT NULL,
        currency      TEXT NOT NULL,
        balance       REAL NOT NULL,
        status        TEXT NOT NULL,
        opened_at     TEXT NOT NULL,
        closed_at     TEXT
    );

    CREATE TABLE transactions (
        transaction_id      TEXT PRIMARY KEY,
        account_id          TEXT NOT NULL REFERENCES accounts(account_id),
        transaction_type    TEXT NOT NULL,
        amount              REAL NOT NULL,
        currency            TEXT NOT NULL,
        counterparty_name   TEXT NOT NULL,
        counterparty_country TEXT NOT NULL,
        description         TEXT NOT NULL,
        transaction_date    TEXT NOT NULL,
        processing_date     TEXT NOT NULL,
        status              TEXT NOT NULL,
        channel             TEXT NOT NULL
    );

    CREATE TABLE rules (
        rule_id                 INTEGER PRIMARY KEY,
        rule_name               TEXT NOT NULL,
        rule_category           TEXT NOT NULL,
        description             TEXT NOT NULL,
        threshold_value         REAL NOT NULL,
        enabled                 INTEGER NOT NULL DEFAULT 1,
        precision_score         REAL NOT NULL,
        recall_score            REAL NOT NULL,
        total_alerts_generated  INTEGER NOT NULL DEFAULT 0,
        true_positives          INTEGER NOT NULL DEFAULT 0,
        false_positives         INTEGER NOT NULL DEFAULT 0,
        last_updated            TEXT NOT NULL
    );

    CREATE TABLE alerts (
        alert_id        TEXT PRIMARY KEY,
        transaction_id  TEXT NOT NULL REFERENCES transactions(transaction_id),
        rule_id         INTEGER NOT NULL REFERENCES rules(rule_id),
        customer_id     TEXT NOT NULL REFERENCES customers(customer_id),
        status          TEXT NOT NULL,
        severity        TEXT NOT NULL,
        created_at      TEXT NOT NULL,
        updated_at      TEXT NOT NULL,
        assigned_to     TEXT,
        notes           TEXT,
        flagged_amount  REAL NOT NULL
    );

    CREATE INDEX idx_txn_account   ON transactions(account_id);
    CREATE INDEX idx_txn_date      ON transactions(transaction_date);
    CREATE INDEX idx_alert_txn     ON alerts(transaction_id);
    CREATE INDEX idx_alert_rule    ON alerts(rule_id);
    CREATE INDEX idx_alert_cust    ON alerts(customer_id);
    CREATE INDEX idx_acct_cust     ON accounts(customer_id);
    """)

    # -------------------------------------------------------- country_risk
    cur.executemany(
        "INSERT INTO country_risk VALUES (?,?,?,?,?)",
        COUNTRY_RISK_DATA,
    )
    conn.commit()

    # -------------------------------------------------------- rules
    RULES = [
        (1,  "Large Cash Transaction",       "structuring",  "Single cash transaction >= threshold",            10000,  1),
        (2,  "Structuring Detection",         "structuring",  "Multiple transactions just below $10k in 24h",    9500,   1),
        (3,  "Rapid Fund Movement",           "velocity",     "Funds moved out within hours of receipt",         0,      1),
        (4,  "High-Risk Country Transfer",    "geographic",   "Wire to/from FATF black/grey-listed country",     0,      1),
        (5,  "Round Dollar Amounts",          "behavior",     "Transaction is an exact round amount",            0,      1),
        (6,  "Dormant Account Activity",      "behavior",     "Account inactive >180 days then transacts",       180,    1),
        (7,  "Multiple Small Deposits",       "structuring",  ">=5 deposits under $3k within 3 days",            3000,   1),
        (8,  "Cross-Border Wire",             "geographic",   "International wire transfer",                     5000,   1),
        (9,  "Velocity Check 24h",            "velocity",     ">=10 transactions in any 24h window",             10,     1),
        (10, "New Account High Value",        "behavior",     "Account <30 days old with txn >$50k",             50000,  1),
        (11, "PEP Transaction",              "network",      "Transaction involving a politically exposed person",0,     1),
        (12, "Sanctions Country",            "geographic",   "Any transaction touching a sanctioned country",    0,      1),
        (13, "Layering Pattern",             "network",      ">=3 sequential transfers across different accounts",0,     1),
        (14, "Smurfing Detection",           "structuring",  "Coordinated small deposits across linked accounts",3000,   1),
        (15, "Business Account Personal Use","behavior",     "Business account used for personal-style spending", 0,     1),
    ]
    rule_rows = []
    for r in RULES:
        rid, rname, rcat, rdesc, rthresh, renabled = r
        precision = round(random.uniform(0.30, 0.90), 4)
        recall    = round(random.uniform(0.40, 0.85), 4)
        tot       = random.randint(50, 800)
        tp        = int(tot * precision)
        fp        = tot - tp
        updated   = ts(random_date(START_DATE, END_DATE))
        rule_rows.append((rid, rname, rcat, rdesc, rthresh, renabled,
                          precision, recall, tot, tp, fp, updated))

    cur.executemany(
        "INSERT INTO rules VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        rule_rows,
    )
    conn.commit()

    # -------------------------------------------------------- customers
    valid_country_codes = [r[0] for r in COUNTRY_RISK_DATA]

    customers = []
    for i in range(1, 501):
        cid       = f"CUST{i:05d}"
        name      = fake.name()
        email     = fake.email()
        dob       = fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat()
        nat       = random.choice(NATIONALITIES)
        kyc       = weighted_choice(KYC_STATUSES, [60, 20, 12, 8])
        # high/critical more common for high-risk nationalities
        if nat in HIGH_RISK_COUNTRIES:
            risk = weighted_choice(RISK_RATINGS, [10, 25, 40, 25])
        else:
            risk = weighted_choice(RISK_RATINGS, [40, 35, 18, 7])
        pep        = 1 if random.random() < 0.04 else 0
        sanctions  = 1 if random.random() < 0.01 else 0
        created    = ts(random_datetime(START_DATE - datetime.timedelta(days=3*365),
                                        START_DATE))
        residence  = random.choice(valid_country_codes)
        customers.append((cid, name, email, dob, nat, kyc, risk,
                          pep, sanctions, created, residence))

    cur.executemany(
        "INSERT INTO customers VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        customers,
    )
    conn.commit()

    customer_ids = [c[0] for c in customers]

    # -------------------------------------------------------- accounts
    # Designate some customers for special fraud patterns
    structuring_customers = set(random.sample(customer_ids, 30))
    highrisk_customers    = set(random.sample(customer_ids, 40))
    dormant_accounts      = []   # filled below
    velocity_accounts     = []   # filled below

    accounts = []
    for i in range(1, 801):
        aid    = f"ACCT{i:06d}"
        cid    = random.choice(customer_ids)
        atype  = weighted_choice(ACCOUNT_TYPES, [45, 35, 20])
        curr   = weighted_choice(CURRENCIES, [50,20,10,5,3,3,3,2,2,2])
        bal    = round(random.uniform(0, 500_000), 2)
        status = weighted_choice(ACCT_STATUSES, [75, 15, 10])
        # opened between 3 years ago and 11 months ago
        opened = random_datetime(START_DATE - datetime.timedelta(days=3*365),
                                 START_DATE - datetime.timedelta(days=30))
        if status == "closed":
            closed = ts(random_datetime(
                datetime.datetime.combine(START_DATE, datetime.time()) +
                datetime.timedelta(days=30),
                datetime.datetime.combine(END_DATE,   datetime.time()),
            ))
        else:
            closed = None
        accounts.append((aid, cid, atype, curr, bal, status, ts(opened), closed))

    cur.executemany(
        "INSERT INTO accounts VALUES (?,?,?,?,?,?,?,?)",
        accounts,
    )
    conn.commit()

    account_ids = [a[0] for a in accounts]
    # Map account_id -> customer_id and currency
    acct_to_cust = {a[0]: a[1] for a in accounts}
    acct_to_curr = {a[0]: a[3] for a in accounts}
    acct_opened  = {a[0]: a[6] for a in accounts}  # ISO string

    # Mark ~30 accounts dormant (inactive for first 6 months, burst at the end)
    dormant_accounts = random.sample(account_ids, 30)
    dormant_set      = set(dormant_accounts)

    # Mark ~20 accounts for velocity spikes
    velocity_accounts = random.sample(account_ids, 20)
    velocity_set      = set(velocity_accounts)

    # -------------------------------------------------------- transactions
    all_country_codes = [r[0] for r in COUNTRY_RISK_DATA]
    high_risk_codes   = [r[0] for r in COUNTRY_RISK_DATA if r[2] in ("high", "critical")]

    descriptions = [
        "Invoice payment", "Supplier payment", "Salary transfer", "Rent payment",
        "Online purchase", "ATM withdrawal", "Utility bill", "Loan repayment",
        "Investment deposit", "Dividend payment", "Consulting fee", "Service charge",
        "Refund", "Wire transfer", "Cash deposit", "Subscription fee",
        "Equipment purchase", "Export proceeds", "Import payment", "Freelance payment",
    ]

    txn_rows  = []
    txn_id_counter = 0

    def make_txn(acct_id, txn_dt, amount=None, cparty_country=None,
                 txn_type=None, channel=None, description=None):
        nonlocal txn_id_counter
        txn_id_counter += 1
        tid    = f"TXN{txn_id_counter:08d}"
        amt    = amount if amount is not None else realistic_amount()
        curr   = acct_to_curr.get(acct_id, "USD")
        cp     = fake.company()
        cpc    = cparty_country if cparty_country else random.choice(all_country_codes)
        desc   = description if description else random.choice(descriptions)
        ttype  = txn_type if txn_type else random.choice(TXN_TYPES)
        tstat  = weighted_choice(TXN_STATUSES, [80, 8, 6, 6])
        chan   = channel if channel else random.choice(CHANNELS)
        proc_dt = txn_dt + datetime.timedelta(hours=random.randint(0, 48))
        if proc_dt > datetime.datetime.combine(END_DATE, datetime.time(23, 59, 59)):
            proc_dt = datetime.datetime.combine(END_DATE, datetime.time(23, 59, 59))
        return (tid, acct_id, ttype, round(amt, 2), curr, cp, cpc,
                desc, ts(txn_dt), ts(proc_dt), tstat, chan)

    # --- Normal transactions spread across the year (base pool)
    TOTAL_TXN = 50_000
    # reserve capacity for patterns
    PATTERN_TXN_BUDGET = 5000
    NORMAL_TXN = TOTAL_TXN - PATTERN_TXN_BUDGET

    active_accounts = [a[0] for a in accounts if a[5] == "active"]
    if not active_accounts:
        active_accounts = account_ids

    # Assign daily transactions roughly uniformly over 365 days
    days_range = (END_DATE - START_DATE).days  # ~365

    for _ in range(NORMAL_TXN):
        aid = random.choice(active_accounts)
        # skip dormant accounts during first 6 months (handled separately)
        if aid in dormant_set:
            # push to last 2 months
            dt = random_datetime(END_DATE - datetime.timedelta(days=60), END_DATE)
        else:
            dt = random_datetime(START_DATE, END_DATE)
        txn_rows.append(make_txn(aid, dt))

    # ---- FRAUD PATTERN 1: Structuring (multiple txns just under $10,000)
    for cid in structuring_customers:
        # find accounts belonging to this customer
        cust_accounts = [a[0] for a in accounts if a[1] == cid and a[5] == "active"]
        if not cust_accounts:
            cust_accounts = [random.choice(active_accounts)]
        for _ in range(random.randint(3, 8)):
            aid = random.choice(cust_accounts)
            dt  = random_datetime(START_DATE, END_DATE)
            amt = round(random.uniform(9_000, 9_999), 2)
            txn_rows.append(make_txn(aid, dt, amount=amt,
                                     description="Cash deposit",
                                     channel="branch"))

    # ---- FRAUD PATTERN 2: Round amounts
    round_amounts = [10_000, 20_000, 25_000, 50_000, 75_000, 100_000, 250_000, 500_000]
    for _ in range(300):
        aid = random.choice(active_accounts)
        dt  = random_datetime(START_DATE, END_DATE)
        amt = random.choice(round_amounts)
        txn_rows.append(make_txn(aid, dt, amount=float(amt)))

    # ---- FRAUD PATTERN 3: High-risk country clusters
    for cid in highrisk_customers:
        cust_accounts = [a[0] for a in accounts if a[1] == cid and a[5] == "active"]
        if not cust_accounts:
            cust_accounts = [random.choice(active_accounts)]
        for _ in range(random.randint(4, 12)):
            aid = random.choice(cust_accounts)
            dt  = random_datetime(START_DATE, END_DATE)
            cpc = random.choice(high_risk_codes)
            txn_rows.append(make_txn(aid, dt, cparty_country=cpc,
                                     txn_type="wire",
                                     description="International wire transfer"))

    # ---- FRAUD PATTERN 4: Velocity spikes (burst of 15-25 txns in 6 hours)
    for aid in velocity_accounts:
        spike_date = random_datetime(START_DATE, END_DATE - datetime.timedelta(days=1))
        for _ in range(random.randint(15, 25)):
            dt = spike_date + datetime.timedelta(minutes=random.randint(0, 360))
            txn_rows.append(make_txn(aid, dt, channel="online"))

    # ---- FRAUD PATTERN 5: Dormant then active (single large burst near end)
    for aid in dormant_accounts:
        burst_start = END_DATE - datetime.timedelta(days=random.randint(10, 50))
        for _ in range(random.randint(5, 15)):
            dt  = random_datetime(burst_start, END_DATE)
            amt = round(random.uniform(5_000, 80_000), 2)
            txn_rows.append(make_txn(aid, dt, amount=amt,
                                     description="Cash deposit",
                                     channel=random.choice(["branch", "online"])))

    # Trim/pad to exactly 50,000
    random.shuffle(txn_rows)
    if len(txn_rows) > TOTAL_TXN:
        txn_rows = txn_rows[:TOTAL_TXN]
    while len(txn_rows) < TOTAL_TXN:
        aid = random.choice(active_accounts)
        dt  = random_datetime(START_DATE, END_DATE)
        txn_rows.append(make_txn(aid, dt))

    cur.executemany(
        "INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        txn_rows,
    )
    conn.commit()

    transaction_ids = [t[0] for t in txn_rows]
    # Map txn_id -> account_id -> customer_id
    txn_to_acct = {t[0]: t[1] for t in txn_rows}
    txn_to_cust = {t[0]: acct_to_cust[t[1]] for t in txn_rows}
    txn_amounts  = {t[0]: t[3] for t in txn_rows}

    # -------------------------------------------------------- rules (ids)
    rule_ids = list(range(1, 16))

    # -------------------------------------------------------- alerts
    ALERT_STATUS_DIST  = ["new"]*40 + ["under_review"]*25 + ["escalated"]*15 \
                       + ["true_positive"]*10 + ["false_positive"]*10
    SEVERITY_DIST      = ["critical"]*20 + ["high"]*30 + ["medium"]*35 + ["low"]*15

    alerts = []
    used_alert_txns = random.choices(transaction_ids, k=2000)

    for i in range(1, 2001):
        aid_str    = f"ALRT{i:06d}"
        tid        = used_alert_txns[i - 1]
        rid        = random.choice(rule_ids)
        cid        = txn_to_cust[tid]
        status     = random.choice(ALERT_STATUS_DIST)
        severity   = random.choice(SEVERITY_DIST)
        created    = random_datetime(START_DATE, END_DATE)
        # updated_at >= created_at
        max_delta  = (datetime.datetime.combine(END_DATE, datetime.time(23,59,59)) - created).seconds
        updated    = created + datetime.timedelta(seconds=random.randint(0, max(max_delta, 1)))
        assigned   = random.choice(ANALYST_NAMES) if status != "new" else None
        flagged    = txn_amounts[tid]
        note_templ = [
            "Flagged by automated rule engine.",
            "Manual review pending.",
            "Customer contacted for clarification.",
            "Escalated to senior analyst.",
            "Confirmed suspicious activity.",
            "Reviewed – no suspicious activity found.",
            "Pattern matches known structuring behaviour.",
            "High-risk counterparty country detected.",
            None,
        ]
        note = random.choice(note_templ)
        alerts.append((aid_str, tid, rid, cid, status, severity,
                       ts(created), ts(updated), assigned, note, flagged))

    cur.executemany(
        "INSERT INTO alerts VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        alerts,
    )
    conn.commit()

    # ---------------------------------------------------------------- summary
    conn.close()
    return DB_PATH


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Generating database …")
    db_path = build_db()

    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()

    tables = ["country_risk", "customers", "accounts", "transactions", "rules", "alerts"]
    print(f"\nDatabase written to: {db_path}\n")
    print(f"{'Table':<20} {'Rows':>10}")
    print("-" * 32)
    total = 0
    for tbl in tables:
        cur.execute(f"SELECT COUNT(*) FROM {tbl}")
        n = cur.fetchone()[0]
        total += n
        print(f"{tbl:<20} {n:>10,}")
    print("-" * 32)
    print(f"{'TOTAL':<20} {total:>10,}")

    # Quick sanity checks
    print("\nSanity checks:")
    cur.execute("SELECT COUNT(DISTINCT customer_id) FROM accounts")
    print(f"  Distinct customers with accounts : {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(DISTINCT account_id) FROM transactions")
    print(f"  Distinct accounts with txns      : {cur.fetchone()[0]}")
    cur.execute("SELECT MIN(transaction_date), MAX(transaction_date) FROM transactions")
    mn, mx = cur.fetchone()
    print(f"  Transaction date range           : {mn[:10]}  to  {mx[:10]}")
    cur.execute("SELECT status, COUNT(*) FROM alerts GROUP BY status ORDER BY 2 DESC")
    print("  Alert status distribution:")
    for row in cur.fetchall():
        print(f"    {row[0]:<16} {row[1]:>5}")
    cur.execute("SELECT severity, COUNT(*) FROM alerts GROUP BY severity ORDER BY 2 DESC")
    print("  Alert severity distribution:")
    for row in cur.fetchall():
        print(f"    {row[0]:<16} {row[1]:>5}")
    cur.execute("SELECT risk_level, COUNT(*) FROM country_risk GROUP BY risk_level ORDER BY 2 DESC")
    print("  Country risk levels:")
    for row in cur.fetchall():
        print(f"    {row[0]:<16} {row[1]:>5}")
    conn.close()
    print("\nDone.")
