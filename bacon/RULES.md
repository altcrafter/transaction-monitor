# AML Rule Engine Reference

The rule engine (`rule_engine.py`) evaluates 17 configurable rules across 5 categories against all enriched transactions. Rules are stored in the `rules` table, making them database-driven and configurable without code changes.

## Scoring Model

Each alert receives a composite risk score (0–100):

```
composite = 100 × (0.30 × rule_severity
                 + 0.25 × customer_risk_norm
                 + 0.25 × geographic_risk_norm
                 + 0.20 × behavioral_score)
```

| Component                | Weight | Source                                            |
|--------------------------|--------|---------------------------------------------------|
| Rule severity            |  30%   | `rules.severity` (configured per rule, 0–1)       |
| Customer risk            |  25%   | Normalized `customers.risk_rating` (1→0.2, 2→0.6, 3→1.0) |
| Geographic risk          |  25%   | Normalized `country_risk.risk_level` (1→0.1 … 5→1.0) |
| Behavioral anomaly       |  20%   | Computed from velocity, amount size, round amounts, new counterparties |

Score bands:
- **60–100:** High (critical, priority investigation)
- **40–59:**  Medium (elevated risk)
- **0–39:**   Low (informational)

---

## Rules by Category

### Structuring (4 rules)

Structuring is the practice of breaking up transactions to avoid reporting thresholds (in the US, cash transactions ≥ $10,000 require a Currency Transaction Report).

| Rule ID | Name | Severity | Description |
|---------|------|----------|-------------|
| STR-001 | Sub-threshold cash deposit | 0.80 | Single cash deposit in $9,000–$9,999 range — just below CTR threshold |
| STR-002 | Aggregate daily sub-threshold deposits | 0.85 | Total cash deposits on account exceed $9,000 in 24h, each below $10,000 |
| STR-003 | Cross-account smurfing | 0.90 | Sub-$10k cash deposits ($8,500–$9,999) across multiple accounts same day |
| STR-004 | Declining cash deposit pattern | 0.75 | Cash deposits ($7,000–$9,999) with 5+ transactions in past 7 days |

**Performance (ground truth):**
- STR-001: 218 alerts, **100% precision** (all on known fraud customers)
- STR-003: 6 alerts, **100% precision**
- STR-002: 1,947 alerts, ~9% precision (broader condition catches legit customers)

---

### Velocity (3 rules)

Velocity rules detect abnormal transaction frequency, a common indicator of account takeover, money mule activity, or automated fraud.

| Rule ID | Name | Severity | Description |
|---------|------|----------|-------------|
| VEL-001 | Rapid transaction burst | 0.85 | 20+ transactions from same account in 1 hour |
| VEL-002 | High 24h transaction count | 0.70 | 40+ transactions on account in any 24-hour window |
| VEL-003 | Volume spike — 24h amount anomaly | 0.75 | Account moves >$100,000 USD in 24 hours |

**Performance:**
- VEL-001: 192 alerts, **100% precision** (all burst patterns on fraud accounts)
- VEL-002: 65 alerts, ~34% precision
- VEL-003: 10,493 alerts, ~8% precision (many legitimate business accounts)

---

### Geographic (4 rules)

Geographic rules flag transactions involving high-risk, very-high-risk, or sanctioned jurisdictions, especially when combined with new counterparty relationships.

| Rule ID | Name | Severity | Description |
|---------|------|----------|-------------|
| GEO-001 | Sanctioned country transaction | 0.95 | Any transaction to/from a sanctioned country (risk=5: IR, KP, RU, CU) |
| GEO-002 | Very high risk country | 0.80 | Transaction involving very-high-risk jurisdiction (risk=4: MM, AF, YE…) |
| GEO-003 | High-risk country new counterparty | 0.70 | First transaction to new counterparty in country with risk ≥ 3 |
| GEO-004 | Multi-country activity — new countries | 0.70 | New counterparties in medium+ risk countries with 5+ txns in 7 days |

**Performance:**
- GEO-001: 9 alerts, **100% precision** — highest severity rule
- GEO-002: 24 alerts, **100% precision**
- GEO-003: 17,813 alerts, ~7% precision (high volume, many normal international transactions)

---

### Behavioral (4 rules)

Behavioral rules detect anomalous patterns that deviate from expected customer behavior: unusual amounts, dormant account reactivation, round-trip transfers, and PEP activity.

| Rule ID | Name | Severity | Description |
|---------|------|----------|-------------|
| BEH-001 | Unusual large amount | 0.80 | Individual customer transaction > $50,000 USD |
| BEH-002 | Dormant account sudden activity | 0.75 | Account with 0 txns in 7 days suddenly moves >$10,000 |
| BEH-003 | Round-amount large wire | 0.65 | Wire transfer with round amount > $25,000 (layering indicator) |
| BEH-004 | PEP high-value transaction | 0.85 | Politically Exposed Person with transaction > $25,000 |

**Performance:**
- BEH-001: 148 alerts, ~92% precision (fairly tight condition)
- BEH-004: 707 alerts, ~8% precision (many PEPs have legitimate large transactions)

---

### Threshold (2 rules)

Simple threshold rules based on regulatory reporting requirements (CTR, international wire monitoring).

| Rule ID | Name | Severity | Description |
|---------|------|----------|-------------|
| THR-001 | Large cash transaction | 0.60 | Cash transaction ≥ $10,000 USD (CTR reporting threshold) |
| THR-002 | Large international wire | 0.65 | International wire > $50,000 USD |

---

## Summary Statistics

| Category      | Rules | Total Alerts | Avg Precision |
|--------------|-------|-------------|---------------|
| Structuring  | 4     | 2,200       | ~76%          |
| Velocity     | 3     | 10,750      | ~47%          |
| Geographic   | 4     | 23,069      | ~29%          |
| Behavioral   | 4     | 7,622       | ~29%          |
| Threshold    | 2     | 4,681       | ~41%          |
| **Total**    | **17**| **48,394**  | —             |

---

## Adding a New Rule

Insert a row into the `rules` table:

```sql
INSERT INTO rules (name, description, category, sql_condition, severity, is_active)
VALUES (
    'STR-005 Rapid sub-threshold pattern',
    'More than 10 sub-threshold deposits in 7 days',
    'structuring',
    't.transaction_type = ''cash_deposit''
     AND te.amount_usd < 10000
     AND te.velocity_7d > 10',
    0.88,
    1
);
```

The `sql_condition` is injected into:
```sql
SELECT ... FROM transactions t
JOIN transaction_enrichment te ON te.transaction_id = t.id
JOIN accounts a ON a.id = t.account_id
JOIN customers c ON c.id = a.customer_id
WHERE <sql_condition>
  AND t.status = 'completed'
```

Available aliases: `t` (transactions), `te` (transaction_enrichment), `a` (accounts), `c` (customers).
