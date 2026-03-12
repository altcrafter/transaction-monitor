-- Transaction Monitoring Platform — SQLite Schema
-- Phase 1: Database Design
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

-- ============================================================
-- COUNTRY RISK TABLE (seed data embedded)
-- ============================================================
CREATE TABLE IF NOT EXISTS country_risk (
    country_code    TEXT PRIMARY KEY,           -- ISO 3166-1 alpha-2
    country_name    TEXT NOT NULL,
    risk_level      INTEGER NOT NULL            -- 1=low, 2=medium, 3=high, 4=very_high, 5=sanctioned
                    CHECK (risk_level BETWEEN 1 AND 5),
    fatf_listed     INTEGER NOT NULL DEFAULT 0  -- 1=on FATF grey/black list
                    CHECK (fatf_listed IN (0, 1)),
    notes           TEXT
);

-- ============================================================
-- CUSTOMERS
-- ============================================================
CREATE TABLE IF NOT EXISTS customers (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    name                TEXT NOT NULL,
    customer_type       TEXT NOT NULL
                        CHECK (customer_type IN ('individual', 'business', 'financial_institution')),
    risk_rating         INTEGER NOT NULL
                        CHECK (risk_rating IN (1, 2, 3)),  -- 1=low, 2=medium, 3=high
    country             TEXT NOT NULL REFERENCES country_risk(country_code),
    registration_date   TEXT NOT NULL,   -- ISO 8601 date
    kyc_status          TEXT NOT NULL
                        CHECK (kyc_status IN ('verified', 'pending', 'expired', 'failed')),
    pep_status          INTEGER NOT NULL DEFAULT 0
                        CHECK (pep_status IN (0, 1)),  -- politically exposed person
    created_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_customers_country     ON customers(country);
CREATE INDEX IF NOT EXISTS idx_customers_risk_rating ON customers(risk_rating);
CREATE INDEX IF NOT EXISTS idx_customers_pep_status  ON customers(pep_status);

-- ============================================================
-- ACCOUNTS
-- ============================================================
CREATE TABLE IF NOT EXISTS accounts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id     INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    account_type    TEXT NOT NULL
                    CHECK (account_type IN ('checking', 'savings', 'business', 'investment', 'crypto')),
    currency        TEXT NOT NULL DEFAULT 'USD'
                    CHECK (length(currency) = 3),
    opened_date     TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'frozen', 'closed')),
    daily_limit     REAL NOT NULL CHECK (daily_limit > 0),
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_accounts_customer_id ON accounts(customer_id);
CREATE INDEX IF NOT EXISTS idx_accounts_status      ON accounts(status);
CREATE INDEX IF NOT EXISTS idx_accounts_currency    ON accounts(currency);

-- ============================================================
-- TRANSACTIONS
-- ============================================================
CREATE TABLE IF NOT EXISTS transactions (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id              INTEGER NOT NULL REFERENCES accounts(id),
    transaction_type        TEXT NOT NULL
                            CHECK (transaction_type IN (
                                'deposit', 'withdrawal', 'transfer_in', 'transfer_out',
                                'wire_in', 'wire_out', 'cash_deposit', 'cash_withdrawal',
                                'crypto_buy', 'crypto_sell'
                            )),
    amount                  REAL NOT NULL CHECK (amount > 0),
    currency                TEXT NOT NULL CHECK (length(currency) = 3),
    timestamp               TEXT NOT NULL,   -- ISO 8601 datetime
    counterparty_account    TEXT,
    counterparty_name       TEXT,
    counterparty_country    TEXT REFERENCES country_risk(country_code),
    channel                 TEXT NOT NULL
                            CHECK (channel IN ('online', 'mobile', 'branch', 'atm', 'wire', 'api')),
    status                  TEXT NOT NULL DEFAULT 'completed'
                            CHECK (status IN ('completed', 'pending', 'failed', 'reversed')),
    created_at              TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_transactions_account_id   ON transactions(account_id);
CREATE INDEX IF NOT EXISTS idx_transactions_timestamp    ON transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_transactions_type         ON transactions(transaction_type);
CREATE INDEX IF NOT EXISTS idx_transactions_amount       ON transactions(amount);
CREATE INDEX IF NOT EXISTS idx_transactions_country      ON transactions(counterparty_country);
CREATE INDEX IF NOT EXISTS idx_transactions_status       ON transactions(status);

-- ============================================================
-- TRANSACTION ENRICHMENT
-- ============================================================
CREATE TABLE IF NOT EXISTS transaction_enrichment (
    transaction_id      INTEGER PRIMARY KEY REFERENCES transactions(id) ON DELETE CASCADE,
    amount_usd          REAL NOT NULL,          -- normalized to USD
    is_round_amount     INTEGER NOT NULL DEFAULT 0
                        CHECK (is_round_amount IN (0, 1)),
    is_large_cash       INTEGER NOT NULL DEFAULT 0  -- >= 10000 USD cash
                        CHECK (is_large_cash IN (0, 1)),
    velocity_1h         INTEGER NOT NULL DEFAULT 0, -- txn count last 1h on account
    velocity_24h        INTEGER NOT NULL DEFAULT 0,
    velocity_7d         INTEGER NOT NULL DEFAULT 0,
    amount_velocity_24h REAL NOT NULL DEFAULT 0,    -- total USD amount last 24h
    country_risk_score  INTEGER NOT NULL DEFAULT 1
                        CHECK (country_risk_score BETWEEN 1 AND 5),
    is_new_counterparty INTEGER NOT NULL DEFAULT 0
                        CHECK (is_new_counterparty IN (0, 1)),
    account_age_days    INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_enrichment_is_large_cash ON transaction_enrichment(is_large_cash);
CREATE INDEX IF NOT EXISTS idx_enrichment_velocity_1h   ON transaction_enrichment(velocity_1h);
CREATE INDEX IF NOT EXISTS idx_enrichment_velocity_24h  ON transaction_enrichment(velocity_24h);
CREATE INDEX IF NOT EXISTS idx_enrichment_country_risk  ON transaction_enrichment(country_risk_score);

-- ============================================================
-- RULES
-- ============================================================
CREATE TABLE IF NOT EXISTS rules (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL UNIQUE,
    description     TEXT NOT NULL,
    category        TEXT NOT NULL
                    CHECK (category IN ('structuring', 'velocity', 'geographic', 'behavioral', 'threshold')),
    sql_condition   TEXT NOT NULL,   -- SQL fragment evaluated in rule engine
    severity        REAL NOT NULL
                    CHECK (severity BETWEEN 0.0 AND 1.0),
    is_active       INTEGER NOT NULL DEFAULT 1
                    CHECK (is_active IN (0, 1)),
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_rules_category  ON rules(category);
CREATE INDEX IF NOT EXISTS idx_rules_is_active ON rules(is_active);

-- ============================================================
-- ALERTS
-- ============================================================
CREATE TABLE IF NOT EXISTS alerts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id  INTEGER NOT NULL REFERENCES transactions(id),
    rule_id         INTEGER NOT NULL REFERENCES rules(id),
    score           REAL NOT NULL CHECK (score BETWEEN 0.0 AND 100.0),
    status          TEXT NOT NULL DEFAULT 'open'
                    CHECK (status IN ('open', 'investigating', 'closed_tp', 'closed_fp', 'escalated')),
    created_date    TEXT NOT NULL DEFAULT (datetime('now')),
    resolved_date   TEXT,
    notes           TEXT,
    UNIQUE(transaction_id, rule_id)
);

CREATE INDEX IF NOT EXISTS idx_alerts_transaction_id ON alerts(transaction_id);
CREATE INDEX IF NOT EXISTS idx_alerts_rule_id        ON alerts(rule_id);
CREATE INDEX IF NOT EXISTS idx_alerts_status         ON alerts(status);
CREATE INDEX IF NOT EXISTS idx_alerts_score          ON alerts(score);
CREATE INDEX IF NOT EXISTS idx_alerts_created_date   ON alerts(created_date);

-- ============================================================
-- ALERT SCORES (component breakdown)
-- ============================================================
CREATE TABLE IF NOT EXISTS alert_scores (
    alert_id                INTEGER PRIMARY KEY REFERENCES alerts(id) ON DELETE CASCADE,
    rule_score              REAL NOT NULL CHECK (rule_score BETWEEN 0.0 AND 1.0),
    customer_risk_score     REAL NOT NULL CHECK (customer_risk_score BETWEEN 0.0 AND 1.0),
    geographic_risk_score   REAL NOT NULL CHECK (geographic_risk_score BETWEEN 0.0 AND 1.0),
    behavioral_score        REAL NOT NULL CHECK (behavioral_score BETWEEN 0.0 AND 1.0),
    composite_score         REAL NOT NULL CHECK (composite_score BETWEEN 0.0 AND 100.0)
);

-- ============================================================
-- AUDIT LOG
-- ============================================================
CREATE TABLE IF NOT EXISTS audit_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type      TEXT NOT NULL
                    CHECK (event_type IN (
                        'schema_init', 'data_loaded', 'etl_run', 'rule_engine_run',
                        'alert_created', 'alert_updated', 'rule_updated', 'export',
                        'api_request', 'system_event'
                    )),
    entity_type     TEXT,   -- 'transaction', 'alert', 'rule', etc.
    entity_id       INTEGER,
    description     TEXT NOT NULL,
    metadata        TEXT,   -- JSON blob
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_audit_log_event_type ON audit_log(event_type);
CREATE INDEX IF NOT EXISTS idx_audit_log_created_at ON audit_log(created_at);

-- ============================================================
-- RULE PERFORMANCE (populated by analysis)
-- ============================================================
CREATE TABLE IF NOT EXISTS rule_performance (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id             INTEGER NOT NULL REFERENCES rules(id),
    evaluation_date     TEXT NOT NULL DEFAULT (date('now')),
    total_alerts        INTEGER NOT NULL DEFAULT 0,
    true_positives      INTEGER NOT NULL DEFAULT 0,
    false_positives     INTEGER NOT NULL DEFAULT 0,
    precision_rate      REAL,   -- tp / (tp + fp), NULL if no closed alerts
    avg_score           REAL,
    median_score        REAL,
    p95_score           REAL,
    notes               TEXT,
    UNIQUE(rule_id, evaluation_date)
);

CREATE INDEX IF NOT EXISTS idx_rule_perf_rule_id ON rule_performance(rule_id);

-- ============================================================
-- SEED DATA: COUNTRY RISK
-- ============================================================
INSERT OR IGNORE INTO country_risk (country_code, country_name, risk_level, fatf_listed, notes) VALUES
-- Low risk (1) — FATF members with strong AML frameworks
('US', 'United States',         1, 0, 'Strong AML/CFT framework'),
('GB', 'United Kingdom',        1, 0, 'Strong AML/CFT framework'),
('DE', 'Germany',               1, 0, 'EU member, strong oversight'),
('FR', 'France',                1, 0, 'EU member, strong oversight'),
('CA', 'Canada',                1, 0, 'FATF founding member'),
('AU', 'Australia',             1, 0, 'Strong AML framework'),
('JP', 'Japan',                 1, 0, 'Strong financial regulation'),
('SG', 'Singapore',             1, 0, 'Major financial hub, strong controls'),
('CH', 'Switzerland',           1, 0, 'Robust banking regulation'),
('NL', 'Netherlands',           1, 0, 'EU member, strong oversight'),
('SE', 'Sweden',                1, 0, 'Transparent financial system'),
('NO', 'Norway',                1, 0, 'Transparent financial system'),
-- Medium risk (2) — adequate but imperfect frameworks
('MX', 'Mexico',                2, 0, 'Drug cartel financial risk'),
('BR', 'Brazil',                2, 0, 'Corruption and drug trafficking risk'),
('IN', 'India',                 2, 0, 'Large cash economy'),
('ZA', 'South Africa',          2, 0, 'Financial crime risk'),
('AR', 'Argentina',             2, 0, 'Currency controls, informality'),
('TR', 'Turkey',                2, 0, 'Geographic crossroads, compliance gaps'),
('AE', 'UAE',                   2, 0, 'Financial hub, trade-based ML risk'),
('HK', 'Hong Kong',             2, 0, 'Trade-based money laundering risk'),
-- High risk (3) — significant AML deficiencies
('NG', 'Nigeria',               3, 1, 'FATF grey list, corruption risk'),
('PK', 'Pakistan',              3, 1, 'FATF grey list'),
('VN', 'Vietnam',               3, 0, 'Weak AML enforcement'),
('KE', 'Kenya',                 3, 0, 'Hawala networks, weak oversight'),
('PH', 'Philippines',           3, 1, 'Casino-based money laundering'),
('TH', 'Thailand',              3, 0, 'Drug trafficking, corruption'),
('MA', 'Morocco',               3, 1, 'FATF grey list'),
-- Very high risk (4) — major AML/CFT failures
('MM', 'Myanmar',               4, 1, 'Military junta, drug production'),
('AF', 'Afghanistan',           4, 1, 'Opium, Taliban financing'),
('YE', 'Yemen',                 4, 1, 'Conflict zone, terrorist financing'),
('LY', 'Libya',                 4, 1, 'Conflict zone, weak state'),
('SY', 'Syria',                 4, 1, 'Conflict zone, sanctions'),
('VE', 'Venezuela',             4, 1, 'Sanctions, corruption, hyperinflation'),
-- Sanctioned (5) — OFAC SDN / comprehensive sanctions
('IR', 'Iran',                  5, 1, 'OFAC comprehensive sanctions'),
('KP', 'North Korea',           5, 1, 'OFAC comprehensive sanctions, WMD'),
('RU', 'Russia',                5, 0, 'OFAC sectoral sanctions, Ukraine war'),
('CU', 'Cuba',                  5, 1, 'OFAC comprehensive sanctions');
