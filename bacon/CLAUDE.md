# Transaction Monitor — Backend Engine

## Project Context
This is the **backend engine** of a transaction monitoring platform. It is a portfolio project for a junior data analyst / compliance analyst job search. The developer has strong math (through abstract algebra) and Python skills (complex Balatro modding).

**DO NOT RUSH.** Each phase should involve building, testing, iterating, and refining. When you generate data, inspect it. When you write queries, run them. Spend time making things GOOD.

---

## PHASE 1 — DATABASE (15+ min)
Design SQLite schema with 10 tables:
- `customers` (id, name, type, risk_rating, country, registration_date, kyc_status, pep_status)
- `accounts` (id, customer_id, account_type, currency, opened_date, status, daily_limit)
- `transactions` (id, account_id, transaction_type, amount, currency, timestamp, counterparty_account, counterparty_name, counterparty_country, channel, status)
- `transaction_enrichment` (transaction_id, amount_usd, is_round_amount, is_large_cash, velocity_1h, velocity_24h, velocity_7d, country_risk_score, is_new_counterparty)
- `rules` (id, name, description, category, sql_condition, severity, is_active)
- `alerts` (id, transaction_id, rule_id, score, status, created_date, resolved_date)
- `alert_scores` (alert_id, rule_score, customer_risk_score, geographic_risk_score, behavioral_score, composite_score)
- `audit_log`
- `country_risk` (30+ real countries with accurate risk levels)
- `rule_performance`

Proper indexes, foreign keys, CHECK constraints. Write `schema.sql` and `db.py`. Test it.

---

## PHASE 2 — DATA GENERATION (30+ min)
Build `data_generator.py` producing:
- 2000 customers
- 5000 accounts
- 200,000+ transactions over 12 months

Realistic distributions: daily/weekly seasonality, power-law amounts, geographic patterns.

**EMBED 8 FRAUD PATTERNS** in at least 50 customers:
1. **Structuring/smurfing** — deposits $9000–$9999
2. **Rapid velocity** — 20+ txns/hour
3. **Round-trip transfers** — out and back same counterparty within 48h
4. **Geographic risk** — sudden sanctioned country activity
5. **Layering** — A→B→C→D transfer chains
6. **Dormant activation** — 6+ months inactive then high volume
7. **Unusual amounts** — 50× normal
8. **Cross-account smurfing** — sub-threshold across multiple accounts same day

Use numpy and Faker. Print distribution stats. Verify and iterate until realistic.

---

## PHASE 3 — ETL (20+ min)
Build `etl.py` that enriches transactions:
- Currency conversion
- Round amount flags
- Large cash flags
- Velocity metrics (1h/24h/7d windows)
- Counterparty country risk lookup
- New counterparty detection
- Account age at transaction time

Batch process in chunks of 1000. Verify enrichment quality with spot checks.

---

## PHASE 4 — RULE ENGINE (30+ min)
Build `rule_engine.py` with 15+ configurable rules across categories:
- **Structuring** (4 rules: sub-threshold, aggregate, cross-account, declining amounts)
- **Velocity** (3 rules: spike detection)
- **Geographic** (3 rules: sanctioned country, new international, multi-country)
- **Behavioral** (3 rules: amount anomaly, dormant activation, round-trip)
- **Threshold** (2 rules)

Store rules in DB. Generate alerts with composite scoring:
```
0.3 × rule_severity + 0.25 × customer_risk + 0.25 × geographic_risk + 0.2 × behavioral_anomaly
```
Normalized 0–100. Print summary. **If any rule gets 0 hits, fix it. Iterate.**

---

## PHASE 5 — API (20+ min)
Build Flask API (`src/api.py`) with 11 endpoints:
- `GET /api/stats`
- `GET /api/transactions` (paginated + filtered)
- `GET /api/alerts` (paginated + filtered)
- `GET /api/alerts/<id>` (full details)
- `GET /api/customers/<id>` (profile + history)
- `GET /api/rules`
- `GET /api/analytics/timeline`
- `GET /api/analytics/risk-distribution`
- `GET /api/analytics/rule-performance`
- `GET /api/analytics/geographic`
- `GET /api/analytics/top-customers`

All JSON. Write and run `api_test.py`. Also write `export.py` for CSV export.

---

## PHASE 6 — TESTING AND DOCS (20+ min)
Write comprehensive `test_suite.py`:
- Schema tests
- Data integrity
- ETL validation
- Rule engine verification
- API tests

Run until all pass. Write `SCHEMA.md` and `RULES.md`. Populate `rule_performance` table with ground truth analysis.

---

## PHASE 7 — ANALYSIS QUERIES (20+ min)
Write `analysis_queries.py` with 20+ SQL queries demonstrating:
- JOINs, window functions, CTEs, subqueries, CASE, HAVING
- Date analysis, self-joins, statistical computations

Queries must include: monthly trends, customer segmentation, alert funnel, resolution time, rule precision, network clusters, structuring detection, geographic flows, time heatmaps, customer lifetime, z-score anomalies, counterparty analysis, velocity comparisons, dormancy analysis, currency exposure, rule co-occurrence, false positive analysis, seasonal patterns, new customer risk, threshold sensitivity.

Each query has a function that runs it and prints formatted results. **Run ALL and verify results make sense.**

---

## PHASE 8 — POLISH (remaining time)
- Add CLI entry point (`cli.py`)
- Optimize slow queries with EXPLAIN
- Full integration test from clean slate
- Write `analysis_report.txt` with key findings

---

## Technical Requirements
- Python 3.10+
- Type hints throughout
- Docstrings on all functions/classes
- Commit after each phase
- Install packages with pip as needed
- SQLite database
