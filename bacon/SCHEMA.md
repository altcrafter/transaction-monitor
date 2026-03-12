# Database Schema Reference

SQLite database located at `data/transactions.db`. All tables created by `schema.sql` and initialized via `db.py`.

## Tables

### `country_risk`
Seed data table — 37 countries with accurate risk levels based on FATF and OFAC guidance.

| Column       | Type    | Constraints                      | Description                              |
|-------------|---------|-----------------------------------|------------------------------------------|
| country_code | TEXT    | PK                                | ISO 3166-1 alpha-2 code                 |
| country_name | TEXT    | NOT NULL                          | Full country name                        |
| risk_level   | INTEGER | NOT NULL, CHECK (1–5)             | 1=Low, 2=Medium, 3=High, 4=Very High, 5=Sanctioned |
| fatf_listed  | INTEGER | NOT NULL DEFAULT 0, CHECK (0,1)   | 1 if on FATF grey or black list          |
| notes        | TEXT    |                                   | Free-text notes                          |

Risk levels map to real-world AML frameworks:
- **1 (Low):** FATF founding members with strong oversight (US, UK, DE, FR, CA…)
- **2 (Medium):** Adequate but imperfect controls (MX, BR, IN, AE…)
- **3 (High):** Significant AML deficiencies, FATF grey list (NG, PK, PH…)
- **4 (Very High):** Major AML failures, conflict zones (MM, AF, SY…)
- **5 (Sanctioned):** OFAC comprehensive sanctions (IR, KP, RU, CU)

---

### `customers`
2,000 synthetic customers (individuals, businesses, financial institutions).

| Column            | Type    | Constraints                                 | Description                    |
|------------------|---------|----------------------------------------------|--------------------------------|
| id               | INTEGER | PK AUTOINCREMENT                             | Surrogate key                  |
| name             | TEXT    | NOT NULL                                     | Customer name (Faker-generated)|
| customer_type    | TEXT    | NOT NULL, CHECK (individual/business/fi)     | Entity type                    |
| risk_rating      | INTEGER | NOT NULL, CHECK (1–3)                        | 1=Low, 2=Medium, 3=High        |
| country          | TEXT    | NOT NULL, FK → country_risk(country_code)    | Home country                   |
| registration_date| TEXT    | NOT NULL                                     | ISO 8601 date                  |
| kyc_status       | TEXT    | NOT NULL, CHECK (verified/pending/expired/failed) | KYC state                 |
| pep_status       | INTEGER | NOT NULL DEFAULT 0, CHECK (0,1)              | Politically Exposed Person flag|
| created_at       | TEXT    | NOT NULL DEFAULT now                         | Row creation timestamp         |

**Indexes:** country, risk_rating, pep_status

---

### `accounts`
~4,290 accounts (1–4 per customer).

| Column       | Type    | Constraints                                     | Description              |
|-------------|---------|--------------------------------------------------|--------------------------|
| id           | INTEGER | PK AUTOINCREMENT                                 | Surrogate key            |
| customer_id  | INTEGER | NOT NULL, FK → customers(id) ON DELETE CASCADE   | Owner                    |
| account_type | TEXT    | NOT NULL, CHECK (checking/savings/business/investment/crypto) | Account category |
| currency     | TEXT    | NOT NULL DEFAULT 'USD', CHECK (len=3)            | ISO 4217 currency        |
| opened_date  | TEXT    | NOT NULL                                         | ISO 8601 date            |
| status       | TEXT    | NOT NULL DEFAULT 'active', CHECK (active/frozen/closed) | Account state       |
| daily_limit  | REAL    | NOT NULL CHECK > 0                               | Daily transaction limit  |
| created_at   | TEXT    | NOT NULL DEFAULT now                             | Row creation timestamp   |

**Indexes:** customer_id, status, currency

---

### `transactions`
198,891 transactions over 2024 (12 months), including 8 embedded fraud patterns.

| Column                | Type    | Constraints                                      | Description                    |
|----------------------|---------|---------------------------------------------------|--------------------------------|
| id                   | INTEGER | PK AUTOINCREMENT                                  | Surrogate key                  |
| account_id           | INTEGER | NOT NULL, FK → accounts(id)                       | Source account                 |
| transaction_type     | TEXT    | NOT NULL, CHECK (deposit/withdrawal/transfer_in/transfer_out/wire_in/wire_out/cash_deposit/cash_withdrawal/crypto_buy/crypto_sell) | Type |
| amount               | REAL    | NOT NULL CHECK > 0                                | Amount in original currency    |
| currency             | TEXT    | NOT NULL CHECK (len=3)                            | ISO 4217 currency              |
| timestamp            | TEXT    | NOT NULL                                          | ISO 8601 datetime              |
| counterparty_account | TEXT    |                                                   | Counterparty account reference |
| counterparty_name    | TEXT    |                                                   | Counterparty name              |
| counterparty_country | TEXT    | FK → country_risk(country_code)                   | Counterparty country           |
| channel              | TEXT    | NOT NULL, CHECK (online/mobile/branch/atm/wire/api) | Transaction channel          |
| status               | TEXT    | NOT NULL DEFAULT 'completed', CHECK (completed/pending/failed/reversed) | Status |
| created_at           | TEXT    | NOT NULL DEFAULT now                              | Row creation timestamp         |

**Indexes:** account_id, timestamp, transaction_type, amount, counterparty_country, status

---

### `transaction_enrichment`
One row per transaction (100% coverage). Populated by `etl.py`.

| Column              | Type    | Constraints          | Description                                |
|--------------------|---------|----------------------|--------------------------------------------|
| transaction_id     | INTEGER | PK, FK → transactions(id) ON DELETE CASCADE | Linked transaction           |
| amount_usd         | REAL    | NOT NULL             | Amount converted to USD (FX-adjusted)      |
| is_round_amount    | INTEGER | NOT NULL, CHECK (0,1)| 1 if amount divisible by 100/1000          |
| is_large_cash      | INTEGER | NOT NULL, CHECK (0,1)| 1 if cash transaction >= $10,000 USD       |
| velocity_1h        | INTEGER | NOT NULL DEFAULT 0   | Count of account txns in preceding 1 hour  |
| velocity_24h       | INTEGER | NOT NULL DEFAULT 0   | Count of account txns in preceding 24 hours|
| velocity_7d        | INTEGER | NOT NULL DEFAULT 0   | Count of account txns in preceding 7 days  |
| amount_velocity_24h| REAL    | NOT NULL DEFAULT 0   | Total USD moved on account in 24 hours     |
| country_risk_score | INTEGER | NOT NULL CHECK (1–5) | Risk level of counterparty country         |
| is_new_counterparty| INTEGER | NOT NULL, CHECK (0,1)| 1 if first transaction to this counterparty|
| account_age_days   | INTEGER | NOT NULL DEFAULT 0   | Days since account opened at txn time      |

**Indexes:** is_large_cash, velocity_1h, velocity_24h, country_risk_score

---

### `rules`
17 active AML detection rules, stored and managed in the database.

| Column        | Type    | Constraints                                              | Description                       |
|--------------|---------|-----------------------------------------------------------|-----------------------------------|
| id           | INTEGER | PK AUTOINCREMENT                                          | Surrogate key                     |
| name         | TEXT    | NOT NULL UNIQUE                                           | Rule identifier (e.g., STR-001)   |
| description  | TEXT    | NOT NULL                                                  | Human-readable description        |
| category     | TEXT    | NOT NULL, CHECK (structuring/velocity/geographic/behavioral/threshold) | Rule category |
| sql_condition| TEXT    | NOT NULL                                                  | SQL WHERE fragment for evaluation |
| severity     | REAL    | NOT NULL CHECK (0–1)                                      | Severity weight for scoring       |
| is_active    | INTEGER | NOT NULL DEFAULT 1, CHECK (0,1)                           | Enabled flag                      |
| created_at   | TEXT    | NOT NULL DEFAULT now                                      | Row creation timestamp            |

---

### `alerts`
48,394 alerts generated by the rule engine.

| Column        | Type    | Constraints                           | Description                        |
|--------------|---------|----------------------------------------|------------------------------------|
| id           | INTEGER | PK AUTOINCREMENT                       | Surrogate key                      |
| transaction_id| INTEGER| NOT NULL, FK → transactions(id)        | Flagged transaction                |
| rule_id      | INTEGER | NOT NULL, FK → rules(id)               | Triggering rule                    |
| score        | REAL    | NOT NULL CHECK (0–100)                 | Composite risk score               |
| status       | TEXT    | NOT NULL DEFAULT 'open', CHECK (open/investigating/closed_tp/closed_fp/escalated) | Investigation status |
| created_date | TEXT    | NOT NULL DEFAULT now                   | Alert creation timestamp           |
| resolved_date| TEXT    |                                        | Resolution timestamp (nullable)    |
| notes        | TEXT    |                                        | Analyst notes                      |
|              |         | UNIQUE(transaction_id, rule_id)        | No duplicate alerts per rule       |

**Indexes:** transaction_id, rule_id, status, score, created_date

---

### `alert_scores`
Score component breakdown for every alert (for explainability).

| Column               | Type    | Constraints             | Description                          |
|---------------------|---------|--------------------------|--------------------------------------|
| alert_id            | INTEGER | PK, FK → alerts(id) ON DELETE CASCADE | Linked alert                |
| rule_score          | REAL    | NOT NULL CHECK (0–1)     | Rule severity component              |
| customer_risk_score | REAL    | NOT NULL CHECK (0–1)     | Customer risk component              |
| geographic_risk_score| REAL   | NOT NULL CHECK (0–1)     | Geographic risk component            |
| behavioral_score    | REAL    | NOT NULL CHECK (0–1)     | Behavioral anomaly component         |
| composite_score     | REAL    | NOT NULL CHECK (0–100)   | Final weighted composite             |

**Scoring formula:** `composite = 100 × (0.30×rule + 0.25×customer + 0.25×geo + 0.20×behavioral)`

---

### `audit_log`
System events log for traceability.

| Column      | Type    | Constraints        | Description                           |
|------------|---------|---------------------|---------------------------------------|
| id         | INTEGER | PK AUTOINCREMENT    | Surrogate key                         |
| event_type | TEXT    | NOT NULL, CHECK(…)  | Event category                        |
| entity_type| TEXT    |                     | Related entity type                   |
| entity_id  | INTEGER |                     | Related entity ID                     |
| description| TEXT    | NOT NULL            | Human-readable description            |
| metadata   | TEXT    |                     | JSON blob with additional context     |
| created_at | TEXT    | NOT NULL DEFAULT now| Event timestamp                       |

Event types: schema_init, data_loaded, etl_run, rule_engine_run, alert_created, alert_updated, rule_updated, export, api_request, system_event

---

### `rule_performance`
Rule evaluation metrics, populated by `tests/test_suite.py`.

| Column         | Type    | Constraints                       | Description                        |
|---------------|---------|------------------------------------|------------------------------------|
| id            | INTEGER | PK AUTOINCREMENT                   | Surrogate key                      |
| rule_id       | INTEGER | NOT NULL, FK → rules(id)           | Evaluated rule                     |
| evaluation_date| TEXT   | NOT NULL DEFAULT today             | Date of evaluation                 |
| total_alerts  | INTEGER | NOT NULL DEFAULT 0                 | Total alerts generated             |
| true_positives| INTEGER | NOT NULL DEFAULT 0                 | TP count (fraud customer alerts)   |
| false_positives| INTEGER| NOT NULL DEFAULT 0                 | FP count                           |
| precision_rate| REAL    |                                    | TP / (TP + FP), NULL if no closed  |
| avg_score     | REAL    |                                    | Mean composite score               |
| median_score  | REAL    |                                    | Median composite score             |
| p95_score     | REAL    |                                    | 95th percentile score              |
| notes         | TEXT    |                                    | Analyst notes                      |
|               |         | UNIQUE(rule_id, evaluation_date)   | One record per rule per day        |

---

## Entity Relationships

```
country_risk ──< customers ──< accounts ──< transactions >── transaction_enrichment
                                                   │
                                                 alerts >── alert_scores
                                                   │
                                                 rules >── rule_performance
```
