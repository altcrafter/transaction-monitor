"""
Generate SQL Analytics Showcase page for TransactGuard AML portfolio project.
Reads from data/transactions.db and outputs output/sql_playground.html.
"""

import sqlite3
import json
import os
import html
from datetime import datetime

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "transactions.db")
OUTPUT_PATH = os.path.join(BASE_DIR, "output", "sql_playground.html")

# ---------------------------------------------------------------------------
# Query definitions
# ---------------------------------------------------------------------------

QUERIES = [
    # ── Window Functions ──────────────────────────────────────────────────
    {
        "id": 1,
        "category": "window",
        "category_label": "Window Functions",
        "title": "Customer Transaction Ranking",
        "business_question": "What is each customer's largest transaction relative to their own history?",
        "sql": """\
SELECT
    c.customer_id,
    c.name                                       AS customer_name,
    t.transaction_id,
    t.amount,
    t.transaction_date,
    RANK() OVER (
        PARTITION BY c.customer_id
        ORDER BY t.amount DESC
    )                                            AS amount_rank,
    COUNT(*) OVER (PARTITION BY c.customer_id)  AS total_txns,
    ROUND(t.amount /
          SUM(t.amount) OVER (PARTITION BY c.customer_id) * 100, 2
    )                                            AS pct_of_customer_total
FROM transactions t
JOIN accounts     a ON t.account_id   = a.account_id
JOIN customers    c ON a.customer_id  = c.customer_id
QUALIFY amount_rank = 1   -- top transaction per customer""",
        "sql_sqlite": """\
WITH ranked AS (
    SELECT
        c.customer_id,
        c.name                                          AS customer_name,
        t.transaction_id,
        t.amount,
        t.transaction_date,
        RANK() OVER (
            PARTITION BY c.customer_id
            ORDER BY t.amount DESC
        )                                               AS amount_rank,
        COUNT(*) OVER (PARTITION BY c.customer_id)     AS total_txns,
        ROUND(
            t.amount /
            SUM(t.amount) OVER (PARTITION BY c.customer_id) * 100,
        2)                                              AS pct_of_customer_total
    FROM transactions t
    JOIN accounts  a ON t.account_id  = a.account_id
    JOIN customers c ON a.customer_id = c.customer_id
)
SELECT *
FROM ranked
WHERE amount_rank = 1
ORDER BY amount DESC
LIMIT 10;""",
        "interpretation": (
            "Each row shows the single highest-value transaction for a given customer alongside "
            "what percentage of that customer's lifetime volume it represents. Customers whose top "
            "transaction accounts for an outsized share of their total volume are prime candidates "
            "for enhanced due diligence, as a single dominant payment can indicate structuring risk "
            "or an unusual business event."
        ),
        "chart": "bar",
        "chart_x": "customer_name",
        "chart_y": "amount",
        "chart_label": "Largest Transaction (USD)",
    },
    {
        "id": 2,
        "category": "window",
        "category_label": "Window Functions",
        "title": "Rolling 7-Day Transaction Velocity",
        "business_question": "Which accounts show accelerating transaction velocity?",
        "sql": """\
SELECT
    account_id,
    DATE(transaction_date)                         AS txn_day,
    COUNT(*)                                       AS daily_count,
    SUM(COUNT(*)) OVER (
        PARTITION BY account_id
        ORDER BY DATE(transaction_date)
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )                                              AS rolling_7d_count,
    ROUND(SUM(amount) OVER (
        PARTITION BY account_id
        ORDER BY DATE(transaction_date)
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2)                                          AS rolling_7d_amount
FROM transactions
GROUP BY account_id, DATE(transaction_date)
ORDER BY rolling_7d_count DESC;""",
        "sql_sqlite": """\
WITH daily AS (
    SELECT
        account_id,
        DATE(transaction_date)   AS txn_day,
        COUNT(*)                 AS daily_count,
        SUM(amount)              AS daily_amount
    FROM transactions
    GROUP BY account_id, DATE(transaction_date)
)
SELECT
    account_id,
    txn_day,
    daily_count,
    SUM(daily_count) OVER (
        PARTITION BY account_id
        ORDER BY txn_day
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )                            AS rolling_7d_count,
    ROUND(SUM(daily_amount) OVER (
        PARTITION BY account_id
        ORDER BY txn_day
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2)                        AS rolling_7d_amount
FROM daily
ORDER BY rolling_7d_count DESC, rolling_7d_amount DESC
LIMIT 10;""",
        "interpretation": (
            "Rolling 7-day counts smooth out daily noise and surface accounts with sustained "
            "high-frequency activity. Accounts at the top of this list maintained the highest "
            "transaction cadence over any consecutive 7-day window — a pattern associated with "
            "layering in AML typologies. Compliance teams should review these accounts for "
            "circular fund movements."
        ),
        "chart": "bar",
        "chart_x": "account_id",
        "chart_y": "rolling_7d_count",
        "chart_label": "Max 7-Day Transaction Count",
    },
    {
        "id": 3,
        "category": "window",
        "category_label": "Window Functions",
        "title": "Month-over-Month Volume Change",
        "business_question": "How has transaction volume trended month-over-month?",
        "sql": """\
WITH monthly AS (
    SELECT
        strftime('%Y-%m', transaction_date)  AS month,
        COUNT(*)                             AS txn_count,
        ROUND(SUM(amount), 2)                AS total_volume
    FROM transactions
    GROUP BY month
)
SELECT
    month,
    txn_count,
    total_volume,
    LAG(total_volume) OVER (ORDER BY month)   AS prev_month_volume,
    ROUND(
        (total_volume - LAG(total_volume) OVER (ORDER BY month))
        / LAG(total_volume) OVER (ORDER BY month) * 100,
    2)                                         AS mom_pct_change
FROM monthly
ORDER BY month;""",
        "sql_sqlite": """\
WITH monthly AS (
    SELECT
        strftime('%Y-%m', transaction_date)  AS month,
        COUNT(*)                             AS txn_count,
        ROUND(SUM(amount), 2)                AS total_volume
    FROM transactions
    GROUP BY month
)
SELECT
    month,
    txn_count,
    total_volume,
    LAG(total_volume) OVER (ORDER BY month)   AS prev_month_volume,
    ROUND(
        (total_volume - LAG(total_volume) OVER (ORDER BY month))
        / LAG(total_volume) OVER (ORDER BY month) * 100,
    2)                                         AS mom_pct_change
FROM monthly
ORDER BY month
LIMIT 10;""",
        "interpretation": (
            "LAG() enables direct period-over-period comparison without a self-join. Months with "
            "large positive spikes in mom_pct_change warrant deeper investigation — they may reflect "
            "legitimate seasonal patterns or the onset of a coordinated fraud campaign. Negative "
            "months can indicate system outages, customer attrition, or the successful remediation "
            "of a fraud wave."
        ),
        "chart": "line",
        "chart_x": "month",
        "chart_y": "mom_pct_change",
        "chart_label": "MoM Volume Change (%)",
    },
    {
        "id": 4,
        "category": "window",
        "category_label": "Window Functions",
        "title": "Running Total by Customer",
        "business_question": "When did high-value customers hit significant volume milestones?",
        "sql": """\
WITH running AS (
    SELECT
        c.customer_id,
        c.name                          AS customer_name,
        t.transaction_date,
        t.amount,
        SUM(t.amount) OVER (
            PARTITION BY c.customer_id
            ORDER BY t.transaction_date
            ROWS UNBOUNDED PRECEDING
        )                               AS running_total
    FROM transactions t
    JOIN accounts  a ON t.account_id  = a.account_id
    JOIN customers c ON a.customer_id = c.customer_id
)
SELECT
    customer_id,
    customer_name,
    transaction_date,
    ROUND(amount, 2)         AS txn_amount,
    ROUND(running_total, 2)  AS running_total
FROM running
WHERE running_total >= 100000
ORDER BY customer_id, transaction_date
LIMIT 10;""",
        "sql_sqlite": """\
WITH running AS (
    SELECT
        c.customer_id,
        c.name                              AS customer_name,
        t.transaction_date,
        t.amount,
        SUM(t.amount) OVER (
            PARTITION BY c.customer_id
            ORDER BY t.transaction_date
            ROWS UNBOUNDED PRECEDING
        )                                   AS running_total
    FROM transactions t
    JOIN accounts  a ON t.account_id  = a.account_id
    JOIN customers c ON a.customer_id = c.customer_id
)
SELECT
    customer_id,
    customer_name,
    transaction_date,
    ROUND(amount, 2)         AS txn_amount,
    ROUND(running_total, 2)  AS running_total
FROM running
WHERE running_total >= 100000
ORDER BY customer_id, transaction_date
LIMIT 10;""",
        "interpretation": (
            "Running totals expose the exact moment a customer crosses a material volume threshold. "
            "The $100,000 mark is significant in AML contexts: customers crossing it quickly after "
            "account opening may be attempting rapid fund placement before regulatory reporting "
            "triggers. This query can be parameterized per threshold to power automated milestone alerts."
        ),
        "chart": None,
    },
    {
        "id": 5,
        "category": "window",
        "category_label": "Window Functions",
        "title": "Percentile Distribution of Transaction Sizes",
        "business_question": "What is the distribution of transaction sizes across the platform?",
        "sql": """\
WITH percentiles AS (
    SELECT
        amount,
        NTILE(100) OVER (ORDER BY amount)  AS percentile
    FROM transactions
)
SELECT
    percentile,
    ROUND(MIN(amount), 2)  AS min_in_bucket,
    ROUND(MAX(amount), 2)  AS max_in_bucket,
    COUNT(*)               AS txn_count
FROM percentiles
WHERE percentile IN (50, 75, 90, 95, 99)
GROUP BY percentile
ORDER BY percentile;""",
        "sql_sqlite": """\
WITH percentiles AS (
    SELECT
        amount,
        NTILE(100) OVER (ORDER BY amount)  AS percentile
    FROM transactions
)
SELECT
    percentile,
    ROUND(MIN(amount), 2)  AS min_in_bucket,
    ROUND(MAX(amount), 2)  AS max_in_bucket,
    COUNT(*)               AS txn_count
FROM percentiles
WHERE percentile IN (50, 75, 90, 95, 99)
GROUP BY percentile
ORDER BY percentile;""",
        "interpretation": (
            "NTILE(100) partitions all transactions into 100 equal-sized buckets by amount, allowing "
            "us to read off precise percentile thresholds. The p99 value is particularly useful as a "
            "dynamic threshold for large-transaction alerts — it adapts to the actual distribution "
            "rather than relying on static dollar cutoffs that become stale over time."
        ),
        "chart": "bar",
        "chart_x": "percentile",
        "chart_y": "max_in_bucket",
        "chart_label": "Transaction Amount at Percentile",
    },
    # ── CTEs ──────────────────────────────────────────────────────────────
    {
        "id": 6,
        "category": "cte",
        "category_label": "CTEs",
        "title": "Alert Investigation Chain",
        "business_question": "Which customers with critical alerts also have the highest alert frequency?",
        "sql": """\
WITH critical_alerts AS (
    -- Step 1: isolate critical-severity alerts
    SELECT alert_id, transaction_id, customer_id, flagged_amount
    FROM alerts
    WHERE severity = 'critical'
),
customer_alert_counts AS (
    -- Step 2: total alert count per customer (all severities)
    SELECT customer_id,
           COUNT(*)                          AS total_alerts,
           SUM(flagged_amount)               AS total_flagged_amount
    FROM alerts
    GROUP BY customer_id
),
enriched AS (
    -- Step 3: join critical alerts to customer profile + alert stats
    SELECT
        ca.customer_id,
        c.name,
        c.risk_rating,
        c.kyc_status,
        ca.flagged_amount                    AS critical_flagged_amt,
        cac.total_alerts,
        cac.total_flagged_amount,
        ROUND(
            CAST(cac.total_alerts AS REAL) /
            NULLIF((SELECT COUNT(*) FROM alerts), 0) * 100,
        2)                                   AS alert_rate_pct
    FROM critical_alerts ca
    JOIN customers c
        ON ca.customer_id = c.customer_id
    JOIN customer_alert_counts cac
        ON ca.customer_id = cac.customer_id
)
SELECT DISTINCT *
FROM enriched
ORDER BY total_alerts DESC, critical_flagged_amt DESC
LIMIT 10;""",
        "sql_sqlite": """\
WITH critical_alerts AS (
    SELECT alert_id, transaction_id, customer_id, flagged_amount
    FROM alerts
    WHERE severity = 'critical'
),
customer_alert_counts AS (
    SELECT customer_id,
           COUNT(*)                          AS total_alerts,
           ROUND(SUM(flagged_amount), 2)     AS total_flagged_amount
    FROM alerts
    GROUP BY customer_id
),
enriched AS (
    SELECT
        ca.customer_id,
        c.name,
        c.risk_rating,
        c.kyc_status,
        ROUND(ca.flagged_amount, 2)          AS critical_flagged_amt,
        cac.total_alerts,
        cac.total_flagged_amount,
        ROUND(
            CAST(cac.total_alerts AS REAL) /
            (SELECT COUNT(*) FROM alerts) * 100,
        2)                                   AS alert_rate_pct
    FROM critical_alerts ca
    JOIN customers c
        ON ca.customer_id = c.customer_id
    JOIN customer_alert_counts cac
        ON ca.customer_id = cac.customer_id
)
SELECT DISTINCT *
FROM enriched
ORDER BY total_alerts DESC, critical_flagged_amt DESC
LIMIT 10;""",
        "interpretation": (
            "This four-step CTE mirrors the mental workflow an analyst follows during alert triage: "
            "find the worst alerts, look up total exposure, layer in customer profile data, and "
            "compute a platform-relative alert rate. Customers appearing here carry both critical "
            "individual flags and disproportionate overall alert frequency — the highest-priority "
            "investigation targets in any AML queue."
        ),
        "chart": "bar",
        "chart_x": "name",
        "chart_y": "total_alerts",
        "chart_label": "Total Alerts per Customer",
    },
    {
        "id": 7,
        "category": "cte",
        "category_label": "CTEs",
        "title": "Structuring Detection CTE",
        "business_question": "Which customers show classic structuring behavior?",
        "sql": """\
WITH same_day_txns AS (
    -- Find transactions in the structuring range ($8k–$9,999)
    SELECT
        a.customer_id,
        DATE(t.transaction_date)  AS txn_day,
        COUNT(*)                  AS txn_count,
        ROUND(SUM(t.amount), 2)   AS day_total,
        MIN(t.amount)             AS min_amt,
        MAX(t.amount)             AS max_amt
    FROM transactions t
    JOIN accounts a ON t.account_id = a.account_id
    WHERE t.amount BETWEEN 8000 AND 9999
    GROUP BY a.customer_id, DATE(t.transaction_date)
    HAVING COUNT(*) >= 3
),
customer_detail AS (
    SELECT
        s.*,
        c.name,
        c.risk_rating,
        c.kyc_status
    FROM same_day_txns s
    JOIN customers c ON s.customer_id = c.customer_id
)
SELECT *
FROM customer_detail
ORDER BY txn_count DESC, day_total DESC
LIMIT 10;""",
        "sql_sqlite": """\
WITH same_day_txns AS (
    SELECT
        a.customer_id,
        DATE(t.transaction_date)  AS txn_day,
        COUNT(*)                  AS txn_count,
        ROUND(SUM(t.amount), 2)   AS day_total,
        ROUND(MIN(t.amount), 2)   AS min_amt,
        ROUND(MAX(t.amount), 2)   AS max_amt
    FROM transactions t
    JOIN accounts a ON t.account_id = a.account_id
    WHERE t.amount BETWEEN 8000 AND 9999
    GROUP BY a.customer_id, DATE(t.transaction_date)
    HAVING COUNT(*) >= 3
),
customer_detail AS (
    SELECT
        s.*,
        c.name,
        c.risk_rating,
        c.kyc_status
    FROM same_day_txns s
    JOIN customers c ON s.customer_id = c.customer_id
)
SELECT *
FROM customer_detail
ORDER BY txn_count DESC, day_total DESC
LIMIT 10;""",
        "interpretation": (
            "Structuring — deliberately breaking large amounts into sub-$10,000 transactions to "
            "evade Currency Transaction Report thresholds — is a federal crime (31 U.S.C. § 5324). "
            "This CTE isolates days where a single customer placed three or more transactions in the "
            "$8,000–$9,999 range. The combination of high count and tightly clustered amounts is a "
            "strong behavioral signal for SAR filing consideration."
        ),
        "chart": "bar",
        "chart_x": "name",
        "chart_y": "txn_count",
        "chart_label": "Same-Day Sub-$10K Transactions",
    },
    {
        "id": 8,
        "category": "cte",
        "category_label": "CTEs",
        "title": "Customer Tier Analysis",
        "business_question": "How does alert rate vary across customer value tiers?",
        "sql": """\
WITH customer_volume AS (
    SELECT
        c.customer_id,
        c.name,
        c.risk_rating,
        ROUND(SUM(t.amount), 2)   AS lifetime_volume
    FROM customers c
    JOIN accounts  a ON c.customer_id = a.customer_id
    JOIN transactions t ON a.account_id = t.account_id
    GROUP BY c.customer_id, c.name, c.risk_rating
),
tiered AS (
    SELECT *,
        CASE
            WHEN lifetime_volume >= 500000 THEN 'Platinum'
            WHEN lifetime_volume >= 200000 THEN 'Gold'
            WHEN lifetime_volume >= 50000  THEN 'Silver'
            ELSE 'Bronze'
        END AS tier
    FROM customer_volume
),
tier_alerts AS (
    SELECT
        t.tier,
        COUNT(DISTINCT t.customer_id)              AS customers,
        ROUND(AVG(t.lifetime_volume), 2)           AS avg_volume,
        COUNT(a.alert_id)                          AS total_alerts,
        ROUND(
            CAST(COUNT(a.alert_id) AS REAL) /
            NULLIF(COUNT(DISTINCT t.customer_id), 0),
        2)                                         AS avg_alerts_per_customer
    FROM tiered t
    LEFT JOIN alerts a ON t.customer_id = a.customer_id
    GROUP BY t.tier
)
SELECT *
FROM tier_alerts
ORDER BY avg_volume DESC;""",
        "sql_sqlite": """\
WITH customer_volume AS (
    SELECT
        c.customer_id,
        c.name,
        c.risk_rating,
        ROUND(SUM(t.amount), 2)   AS lifetime_volume
    FROM customers c
    JOIN accounts  a ON c.customer_id = a.customer_id
    JOIN transactions t ON a.account_id = t.account_id
    GROUP BY c.customer_id, c.name, c.risk_rating
),
tiered AS (
    SELECT *,
        CASE
            WHEN lifetime_volume >= 500000 THEN 'Platinum'
            WHEN lifetime_volume >= 200000 THEN 'Gold'
            WHEN lifetime_volume >= 50000  THEN 'Silver'
            ELSE 'Bronze'
        END AS tier
    FROM customer_volume
),
tier_alerts AS (
    SELECT
        t.tier,
        COUNT(DISTINCT t.customer_id)              AS customers,
        ROUND(AVG(t.lifetime_volume), 2)           AS avg_volume,
        COUNT(a.alert_id)                          AS total_alerts,
        ROUND(
            CAST(COUNT(a.alert_id) AS REAL) /
            COUNT(DISTINCT t.customer_id),
        2)                                         AS avg_alerts_per_customer
    FROM tiered t
    LEFT JOIN alerts a ON t.customer_id = a.customer_id
    GROUP BY t.tier
)
SELECT *
FROM tier_alerts
ORDER BY avg_volume DESC;""",
        "interpretation": (
            "Segmenting customers into value tiers reveals whether higher-revenue relationships "
            "carry proportionally higher compliance cost. If Platinum customers generate far more "
            "alerts per head than Bronze customers, the institution may need enhanced monitoring "
            "controls specifically tuned to high-value accounts. This analysis informs risk-based "
            "resource allocation across the compliance function."
        ),
        "chart": "bar",
        "chart_x": "tier",
        "chart_y": "avg_alerts_per_customer",
        "chart_label": "Avg Alerts per Customer by Tier",
    },
    # ── Aggregation ───────────────────────────────────────────────────────
    {
        "id": 9,
        "category": "aggregation",
        "category_label": "Aggregation",
        "title": "Multi-Dimensional Alert Summary",
        "business_question": "What is the complete breakdown of alerts across all classification dimensions?",
        "sql": """\
SELECT
    r.rule_category,
    a.severity,
    a.status,
    COUNT(*)                                   AS alert_count,
    ROUND(SUM(a.flagged_amount), 2)            AS total_flagged,
    ROUND(AVG(a.flagged_amount), 2)            AS avg_flagged,
    SUM(CASE WHEN a.severity = 'critical'
             THEN 1 ELSE 0 END)                AS critical_count,
    SUM(CASE WHEN a.status IN
             ('true_positive','escalated')
             THEN 1 ELSE 0 END)                AS confirmed_count
FROM alerts a
JOIN rules r ON a.rule_id = r.rule_id
GROUP BY r.rule_category, a.severity, a.status
ORDER BY total_flagged DESC
LIMIT 10;""",
        "sql_sqlite": """\
SELECT
    r.rule_category,
    a.severity,
    a.status,
    COUNT(*)                                   AS alert_count,
    ROUND(SUM(a.flagged_amount), 2)            AS total_flagged,
    ROUND(AVG(a.flagged_amount), 2)            AS avg_flagged,
    SUM(CASE WHEN a.severity = 'critical'
             THEN 1 ELSE 0 END)                AS critical_count,
    SUM(CASE WHEN a.status IN
             ('true_positive','escalated')
             THEN 1 ELSE 0 END)                AS confirmed_count
FROM alerts a
JOIN rules r ON a.rule_id = r.rule_id
GROUP BY r.rule_category, a.severity, a.status
ORDER BY total_flagged DESC
LIMIT 10;""",
        "interpretation": (
            "A three-dimensional GROUP BY (category × severity × status) produces a compliance "
            "heat-map without requiring a BI tool. The confirmed_count column — derived via "
            "conditional SUM(CASE WHEN) — isolates actionable intelligence from noise. Rule "
            "categories with high total_flagged but low confirmed_count are candidates for "
            "threshold recalibration to reduce false-positive burden."
        ),
        "chart": None,
    },
    {
        "id": 10,
        "category": "aggregation",
        "category_label": "Aggregation",
        "title": "Rule Performance Metrics (Precision / Recall / F1)",
        "business_question": "Which AML rules have the best precision-recall tradeoff?",
        "sql": """\
SELECT
    rule_id,
    rule_name,
    rule_category,
    true_positives,
    false_positives,
    total_alerts_generated,
    ROUND(
        CAST(true_positives AS REAL) /
        NULLIF(true_positives + false_positives, 0),
    4)                               AS precision_sql,
    ROUND(precision_score,  4)       AS precision_stored,
    ROUND(recall_score,     4)       AS recall_stored,
    ROUND(
        2.0 * precision_score * recall_score /
        NULLIF(precision_score + recall_score, 0),
    4)                               AS f1_score
FROM rules
WHERE enabled = 1
ORDER BY f1_score DESC;""",
        "sql_sqlite": """\
SELECT
    rule_id,
    rule_name,
    rule_category,
    true_positives,
    false_positives,
    total_alerts_generated,
    ROUND(
        CAST(true_positives AS REAL) /
        (true_positives + false_positives + 0.0001),
    4)                               AS precision_sql,
    ROUND(precision_score,  4)       AS precision_stored,
    ROUND(recall_score,     4)       AS recall_stored,
    ROUND(
        2.0 * precision_score * recall_score /
        (precision_score + recall_score + 0.0001),
    4)                               AS f1_score
FROM rules
WHERE enabled = 1
ORDER BY f1_score DESC;""",
        "interpretation": (
            "Computing F1 = 2·P·R / (P+R) directly in SQL eliminates the need to export data to "
            "Python or Excel for rule evaluation. Rules with high F1 (>0.7) reliably identify true "
            "suspicious activity; rules below 0.4 generate more noise than signal and should be "
            "reviewed for threshold adjustment or retirement. This query can be scheduled as a "
            "weekly rule-health report."
        ),
        "chart": "bar",
        "chart_x": "rule_name",
        "chart_y": "f1_score",
        "chart_label": "F1 Score by Rule",
    },
    # ── Date Analysis ─────────────────────────────────────────────────────
    {
        "id": 11,
        "category": "date",
        "category_label": "Date Analysis",
        "title": "Day-of-Week & Hour-of-Day Pattern",
        "business_question": "When are high-value transactions most likely to occur?",
        "sql": """\
SELECT
    CASE CAST(strftime('%w', transaction_date) AS INTEGER)
        WHEN 0 THEN 'Sunday'    WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'   WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'  WHEN 5 THEN 'Friday'
        ELSE 'Saturday'
    END                                         AS day_of_week,
    CAST(strftime('%H', transaction_date) AS INTEGER)
                                                AS hour_of_day,
    COUNT(*)                                    AS txn_count,
    ROUND(AVG(amount), 2)                       AS avg_amount,
    ROUND(MAX(amount), 2)                       AS max_amount
FROM transactions
WHERE amount > 10000          -- focus on high-value
GROUP BY day_of_week, hour_of_day
ORDER BY avg_amount DESC
LIMIT 10;""",
        "sql_sqlite": """\
SELECT
    CASE CAST(strftime('%w', transaction_date) AS INTEGER)
        WHEN 0 THEN 'Sunday'    WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'   WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'  WHEN 5 THEN 'Friday'
        ELSE 'Saturday'
    END                                         AS day_of_week,
    CAST(strftime('%H', transaction_date) AS INTEGER)
                                                AS hour_of_day,
    COUNT(*)                                    AS txn_count,
    ROUND(AVG(amount), 2)                       AS avg_amount,
    ROUND(MAX(amount), 2)                       AS max_amount
FROM transactions
WHERE amount > 10000
GROUP BY day_of_week, hour_of_day
ORDER BY avg_amount DESC
LIMIT 10;""",
        "interpretation": (
            "Temporal patterns in high-value transactions reveal operational risk windows. "
            "Peaks during off-hours (late night, early morning) or weekends are anomalous for most "
            "legitimate business models and warrant closer review. Understanding the normal "
            "day/hour distribution also powers time-based alert rules that reduce false positives "
            "during typical business hours."
        ),
        "chart": "bar",
        "chart_x": "day_of_week",
        "chart_y": "avg_amount",
        "chart_label": "Avg High-Value Transaction Amount",
    },
    {
        "id": 12,
        "category": "date",
        "category_label": "Date Analysis",
        "title": "Dormant Account Reactivation",
        "business_question": "Which dormant accounts have recently reactivated?",
        "sql": """\
WITH account_gaps AS (
    SELECT
        account_id,
        transaction_date                          AS current_txn_date,
        LAG(transaction_date) OVER (
            PARTITION BY account_id
            ORDER BY transaction_date
        )                                         AS prev_txn_date,
        amount
    FROM transactions
),
reactivations AS (
    SELECT
        account_id,
        prev_txn_date,
        current_txn_date,
        ROUND(
            JULIANDAY(current_txn_date) -
            JULIANDAY(prev_txn_date),
        0)                                        AS days_dormant,
        ROUND(amount, 2)                          AS reactivation_amount
    FROM account_gaps
    WHERE prev_txn_date IS NOT NULL
      AND JULIANDAY(current_txn_date) -
          JULIANDAY(prev_txn_date) >= 180
)
SELECT *
FROM reactivations
ORDER BY days_dormant DESC
LIMIT 10;""",
        "sql_sqlite": """\
WITH account_gaps AS (
    SELECT
        account_id,
        transaction_date                          AS current_txn_date,
        LAG(transaction_date) OVER (
            PARTITION BY account_id
            ORDER BY transaction_date
        )                                         AS prev_txn_date,
        amount
    FROM transactions
),
reactivations AS (
    SELECT
        account_id,
        prev_txn_date,
        current_txn_date,
        ROUND(
            JULIANDAY(current_txn_date) -
            JULIANDAY(prev_txn_date),
        0)                                        AS days_dormant,
        ROUND(amount, 2)                          AS reactivation_amount
    FROM account_gaps
    WHERE prev_txn_date IS NOT NULL
      AND JULIANDAY(current_txn_date) -
          JULIANDAY(prev_txn_date) >= 180
)
SELECT *
FROM reactivations
ORDER BY days_dormant DESC
LIMIT 10;""",
        "interpretation": (
            "Account dormancy followed by sudden activity is a classic money-laundering indicator: "
            "accounts may be opened, left dormant to avoid scrutiny, then activated when needed "
            "for layering. JULIANDAY() arithmetic gives an exact day-count gap between consecutive "
            "transactions per account. Any reactivation exceeding 180 days should trigger an "
            "automatic enhanced due diligence review."
        ),
        "chart": "bar",
        "chart_x": "account_id",
        "chart_y": "days_dormant",
        "chart_label": "Days Dormant Before Reactivation",
    },
    # ── JOINs ─────────────────────────────────────────────────────────────
    {
        "id": 13,
        "category": "joins",
        "category_label": "JOINs",
        "title": "High-Risk Country Exposure",
        "business_question": "What is the total exposure to high-risk country transactions?",
        "sql": """\
SELECT
    cr.country_name,
    cr.risk_level,
    cr.fatf_status,
    COUNT(t.transaction_id)           AS txn_count,
    ROUND(SUM(t.amount), 2)           AS total_exposure,
    ROUND(AVG(t.amount), 2)           AS avg_txn_amount,
    COUNT(DISTINCT a.customer_id)     AS unique_customers,
    SUM(CASE WHEN c.risk_rating
             IN ('high','critical')
             THEN 1 ELSE 0 END)       AS high_risk_customers
FROM transactions t
JOIN accounts      a  ON t.account_id          = a.account_id
JOIN customers     c  ON a.customer_id         = c.customer_id
JOIN country_risk  cr ON t.counterparty_country = cr.country_code
WHERE cr.risk_level IN ('high', 'critical')
GROUP BY cr.country_name, cr.risk_level, cr.fatf_status
ORDER BY total_exposure DESC
LIMIT 10;""",
        "sql_sqlite": """\
SELECT
    cr.country_name,
    cr.risk_level,
    cr.fatf_status,
    COUNT(t.transaction_id)           AS txn_count,
    ROUND(SUM(t.amount), 2)           AS total_exposure,
    ROUND(AVG(t.amount), 2)           AS avg_txn_amount,
    COUNT(DISTINCT a.customer_id)     AS unique_customers,
    SUM(CASE WHEN c.risk_rating IN ('high','critical')
             THEN 1 ELSE 0 END)       AS high_risk_customers
FROM transactions t
JOIN accounts      a  ON t.account_id           = a.account_id
JOIN customers     c  ON a.customer_id          = c.customer_id
JOIN country_risk  cr ON t.counterparty_country = cr.country_code
WHERE cr.risk_level IN ('high', 'critical')
GROUP BY cr.country_name, cr.risk_level, cr.fatf_status
ORDER BY total_exposure DESC
LIMIT 10;""",
        "interpretation": (
            "A four-table JOIN — transactions → accounts → customers → country_risk — surfaces "
            "total monetary exposure to FATF-listed or otherwise high-risk jurisdictions. The "
            "high_risk_customers column adds a second risk dimension: transactions to sanctioned "
            "countries by already-flagged customers represent the institution's highest-priority "
            "exposure and may require immediate filing with FinCEN or equivalent authorities."
        ),
        "chart": "bar",
        "chart_x": "country_name",
        "chart_y": "total_exposure",
        "chart_label": "Total Exposure by Country (USD)",
    },
    {
        "id": 14,
        "category": "joins",
        "category_label": "JOINs",
        "title": "Alert-to-Transaction Attribution",
        "business_question": "What transaction characteristics most commonly trigger alerts?",
        "sql": """\
SELECT
    r.rule_name,
    r.rule_category,
    a.severity,
    t.transaction_type,
    t.channel,
    COUNT(a.alert_id)                  AS alert_count,
    ROUND(AVG(t.amount), 2)            AS avg_trigger_amount,
    ROUND(AVG(a.flagged_amount), 2)    AS avg_flagged_amount,
    ROUND(MIN(t.amount), 2)            AS min_trigger_amount,
    ROUND(MAX(t.amount), 2)            AS max_trigger_amount
FROM alerts a
JOIN transactions t ON a.transaction_id = t.transaction_id
JOIN rules        r ON a.rule_id        = r.rule_id
GROUP BY r.rule_name, r.rule_category,
         a.severity, t.transaction_type, t.channel
ORDER BY alert_count DESC
LIMIT 10;""",
        "sql_sqlite": """\
SELECT
    r.rule_name,
    r.rule_category,
    a.severity,
    t.transaction_type,
    t.channel,
    COUNT(a.alert_id)                  AS alert_count,
    ROUND(AVG(t.amount), 2)            AS avg_trigger_amount,
    ROUND(AVG(a.flagged_amount), 2)    AS avg_flagged_amount,
    ROUND(MIN(t.amount), 2)            AS min_trigger_amount,
    ROUND(MAX(t.amount), 2)            AS max_trigger_amount
FROM alerts a
JOIN transactions t ON a.transaction_id = t.transaction_id
JOIN rules        r ON a.rule_id        = r.rule_id
GROUP BY r.rule_name, r.rule_category,
         a.severity, t.transaction_type, t.channel
ORDER BY alert_count DESC
LIMIT 10;""",
        "interpretation": (
            "By joining alerts back to their triggering transactions and the rules that fired, "
            "this query builds a profile of 'what a suspicious transaction looks like' under "
            "each rule. The combination of channel, transaction_type, and amount range provides "
            "actionable intelligence for rule tuning: if a rule fires predominantly on mobile "
            "credits under $500, its threshold may be miscalibrated."
        ),
        "chart": None,
    },
    # ── Statistical ───────────────────────────────────────────────────────
    {
        "id": 15,
        "category": "statistical",
        "category_label": "Statistical",
        "title": "Z-Score Anomaly Detection",
        "business_question": "Which days had statistically anomalous transaction volumes?",
        "sql": """\
WITH daily_counts AS (
    SELECT
        DATE(transaction_date)  AS txn_day,
        COUNT(*)                AS daily_txn_count
    FROM transactions
    GROUP BY DATE(transaction_date)
),
stats AS (
    SELECT
        AVG(daily_txn_count)                            AS mean_count,
        -- SQLite: stddev via variance formula
        SQRT(AVG(daily_txn_count * daily_txn_count)
             - AVG(daily_txn_count) * AVG(daily_txn_count))
                                                        AS stddev_count
    FROM daily_counts
)
SELECT
    d.txn_day,
    d.daily_txn_count,
    ROUND(s.mean_count, 2)                             AS mean,
    ROUND(s.stddev_count, 2)                           AS stddev,
    ROUND(
        (d.daily_txn_count - s.mean_count) /
        NULLIF(s.stddev_count, 0),
    2)                                                 AS z_score,
    CASE
        WHEN ABS((d.daily_txn_count - s.mean_count) /
                 NULLIF(s.stddev_count, 0)) > 2
        THEN 'ANOMALOUS'
        ELSE 'normal'
    END                                                AS flag
FROM daily_counts d
CROSS JOIN stats s
ORDER BY ABS(z_score) DESC
LIMIT 10;""",
        "sql_sqlite": """\
WITH daily_counts AS (
    SELECT
        DATE(transaction_date)  AS txn_day,
        COUNT(*)                AS daily_txn_count
    FROM transactions
    GROUP BY DATE(transaction_date)
),
stats AS (
    SELECT
        AVG(daily_txn_count)                            AS mean_count,
        SQRT(
            AVG(daily_txn_count * daily_txn_count)
            - AVG(daily_txn_count) * AVG(daily_txn_count)
        )                                               AS stddev_count
    FROM daily_counts
)
SELECT
    d.txn_day,
    d.daily_txn_count,
    ROUND(s.mean_count, 2)                              AS mean,
    ROUND(s.stddev_count, 2)                            AS stddev,
    ROUND(
        (d.daily_txn_count - s.mean_count) /
        (s.stddev_count + 0.0001),
    2)                                                  AS z_score,
    CASE
        WHEN ABS(
            (d.daily_txn_count - s.mean_count) /
            (s.stddev_count + 0.0001)
        ) > 2 THEN 'ANOMALOUS'
        ELSE 'normal'
    END                                                 AS flag
FROM daily_counts d
CROSS JOIN stats s
ORDER BY ABS(z_score) DESC
LIMIT 10;""",
        "interpretation": (
            "Standard deviation is not a built-in SQLite aggregate, but the population formula "
            "σ = √(E[X²] − (E[X])²) can be expressed entirely using AVG(). This query implements "
            "z-score normalization inline: any day with |z| > 2 lies outside 95% of the normal "
            "distribution. The CROSS JOIN propagates the platform-wide statistics to every row "
            "without a correlated subquery, keeping the plan efficient on large datasets."
        ),
        "chart": "bar",
        "chart_x": "txn_day",
        "chart_y": "z_score",
        "chart_label": "Z-Score (daily transaction count)",
    },
    {
        "id": 16,
        "category": "statistical",
        "category_label": "Statistical",
        "title": "Customer Behavioral Cohort Analysis",
        "business_question": "How do transaction patterns evolve across customer cohorts?",
        "sql": """\
WITH first_txn AS (
    -- Identify each customer's first transaction month (their cohort)
    SELECT
        a.customer_id,
        strftime('%Y-%m', MIN(t.transaction_date))  AS cohort_month
    FROM transactions t
    JOIN accounts a ON t.account_id = a.account_id
    GROUP BY a.customer_id
),
cohort_activity AS (
    SELECT
        ft.cohort_month,
        strftime('%Y-%m', t.transaction_date)       AS activity_month,
        COUNT(DISTINCT a.customer_id)               AS active_customers,
        ROUND(SUM(t.amount), 2)                     AS cohort_volume
    FROM transactions t
    JOIN accounts a ON t.account_id = a.account_id
    JOIN first_txn ft ON a.customer_id = ft.customer_id
    GROUP BY ft.cohort_month, strftime('%Y-%m', t.transaction_date)
)
SELECT
    cohort_month,
    activity_month,
    active_customers,
    cohort_volume,
    ROUND(
        (CAST(
            strftime('%Y', activity_month) AS INTEGER) * 12 +
            CAST(strftime('%m', activity_month) AS INTEGER))
        - (CAST(strftime('%Y', cohort_month) AS INTEGER) * 12 +
           CAST(strftime('%m', cohort_month) AS INTEGER)),
    0)                                              AS months_since_acquisition
FROM cohort_activity
ORDER BY cohort_month, activity_month
LIMIT 10;""",
        "sql_sqlite": """\
WITH first_txn AS (
    SELECT
        a.customer_id,
        strftime('%Y-%m', MIN(t.transaction_date))  AS cohort_month
    FROM transactions t
    JOIN accounts a ON t.account_id = a.account_id
    GROUP BY a.customer_id
),
cohort_activity AS (
    SELECT
        ft.cohort_month,
        strftime('%Y-%m', t.transaction_date)       AS activity_month,
        COUNT(DISTINCT a.customer_id)               AS active_customers,
        ROUND(SUM(t.amount), 2)                     AS cohort_volume
    FROM transactions t
    JOIN accounts a  ON t.account_id  = a.account_id
    JOIN first_txn ft ON a.customer_id = ft.customer_id
    GROUP BY ft.cohort_month, strftime('%Y-%m', t.transaction_date)
)
SELECT
    cohort_month,
    activity_month,
    active_customers,
    cohort_volume,
    ROUND(
        (CAST(strftime('%Y', activity_month) AS INTEGER) * 12
         + CAST(strftime('%m', activity_month) AS INTEGER))
        - (CAST(strftime('%Y', cohort_month) AS INTEGER) * 12
           + CAST(strftime('%m', cohort_month) AS INTEGER)),
    0)                                              AS months_since_acquisition
FROM cohort_activity
ORDER BY cohort_month, activity_month
LIMIT 10;""",
        "interpretation": (
            "Cohort analysis groups customers by acquisition month, then tracks whether each cohort "
            "remains active in subsequent months. Cohorts that retain a high proportion of active "
            "customers months after onboarding indicate healthy engagement; rapidly declining cohorts "
            "may suggest friction in the product experience or account take-over after onboarding. "
            "From a compliance perspective, cohorts acquired during known fraud waves deserve "
            "retrospective review."
        ),
        "chart": "line",
        "chart_x": "cohort_month",
        "chart_y": "active_customers",
        "chart_label": "Active Customers",
    },
]

# ---------------------------------------------------------------------------
# Category metadata
# ---------------------------------------------------------------------------

CATEGORY_META = {
    "window":      {"label": "Window Functions", "color": "#a371f7", "bg": "#2d1f47"},
    "cte":         {"label": "CTEs",             "color": "#58a6ff", "bg": "#1a2a3a"},
    "aggregation": {"label": "Aggregation",      "color": "#3fb950", "bg": "#1a2e1a"},
    "date":        {"label": "Date Analysis",    "color": "#d29922", "bg": "#2e2a10"},
    "joins":       {"label": "JOINs",            "color": "#2ea6a6", "bg": "#102e2e"},
    "statistical": {"label": "Statistical",      "color": "#f85149", "bg": "#2e1010"},
}

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def run_query(conn, sql):
    """Execute SQL and return (columns, rows). Returns empty on error."""
    cur = conn.cursor()
    try:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        return cols, rows
    except Exception as exc:
        print(f"  [WARN] Query failed: {exc}")
        return [], []


def rows_to_dicts(cols, rows):
    return [dict(zip(cols, row)) for row in rows]


# ---------------------------------------------------------------------------
# HTML rendering helpers
# ---------------------------------------------------------------------------

def escape(s):
    return html.escape(str(s)) if s is not None else "—"


def render_table(cols, rows, max_rows=10):
    if not cols:
        return '<p class="no-data">No results returned.</p>'
    display_rows = rows[:max_rows]
    parts = ['<div class="table-wrapper"><table class="results-table">']
    parts.append("<thead><tr>")
    for c in cols:
        parts.append(f'<th>{escape(c)}</th>')
    parts.append("</tr></thead><tbody>")
    for row in display_rows:
        parts.append("<tr>")
        for cell in row:
            parts.append(f'<td>{escape(cell)}</td>')
        parts.append("</tr>")
    parts.append("</tbody></table></div>")
    if len(rows) > max_rows:
        parts.append(
            f'<p class="row-note">Showing {max_rows} of {len(rows)} rows</p>'
        )
    return "".join(parts)


def render_chart(query, cols, rows, chart_id):
    """Render a Chart.js canvas + inline script for a query result."""
    cx = query.get("chart_x")
    cy = query.get("chart_y")
    chart_type = query.get("chart")
    label = query.get("chart_label", cy)

    if not cx or not cy or not chart_type or not cols or cx not in cols or cy not in cols:
        return ""

    xi = cols.index(cx)
    yi = cols.index(cy)

    labels = []
    values = []
    for row in rows[:10]:
        lv = row[xi]
        vv = row[yi]
        labels.append(str(lv) if lv is not None else "N/A")
        try:
            values.append(float(vv) if vv is not None else 0)
        except (ValueError, TypeError):
            values.append(0)

    cat = query["category"]
    accent = CATEGORY_META[cat]["color"]

    labels_json = json.dumps(labels)
    values_json = json.dumps(values)
    chart_type_js = "bar" if chart_type == "bar" else "line"

    if chart_type_js == "line":
        dataset_extra = '"fill": false, "tension": 0.4,'
        border_color = f'"borderColor": "{accent}",'
        bg_color = f'"backgroundColor": "{accent}33",'
    else:
        dataset_extra = ""
        border_color = f'"borderColor": "{accent}",'
        bg_color = (
            f'"backgroundColor": '
            f'["#58a6ff44","#a371f744","#3fb95044","#d2992244",'
            f'"#2ea6a644","#f8514944","#58a6ff44","#a371f744",'
            f'"#3fb95044","#d2992244"],'
        )

    return f"""
<div class="chart-container">
  <canvas id="{chart_id}" height="200"></canvas>
</div>
<script>
(function() {{
  var ctx = document.getElementById('{chart_id}').getContext('2d');
  new Chart(ctx, {{
    type: '{chart_type_js}',
    data: {{
      labels: {labels_json},
      datasets: [{{
        label: {json.dumps(label)},
        data: {values_json},
        {bg_color}
        {border_color}
        {dataset_extra}
        borderWidth: 2,
        borderRadius: 4
      }}]
    }},
    options: {{
      responsive: true,
      maintainAspectRatio: false,
      plugins: {{
        legend: {{ labels: {{ color: '#c9d1d9', font: {{ family: 'Inter' }} }} }},
        tooltip: {{ bodyColor: '#c9d1d9', titleColor: '#58a6ff' }}
      }},
      scales: {{
        x: {{
          ticks: {{ color: '#8b949e', maxRotation: 35, font: {{ size: 10 }} }},
          grid: {{ color: '#21262d' }}
        }},
        y: {{
          ticks: {{ color: '#8b949e' }},
          grid: {{ color: '#21262d' }}
        }}
      }}
    }}
  }});
}})();
</script>"""


def render_query_card(query, cols, rows):
    cat = query["category"]
    meta = CATEGORY_META[cat]
    accent = meta["color"]
    cat_bg = meta["bg"]
    cat_label = meta["label"]

    sql_display = query.get("sql_sqlite", query["sql"])
    sql_escaped = escape(sql_display)

    table_html = render_table(cols, rows)
    chart_id = f"chart-q{query['id']}"
    chart_html = render_chart(query, cols, rows, chart_id)

    row_count = len(rows)
    col_count = len(cols)

    return f"""
<div class="query-card" data-category="{cat}" id="q{query['id']}">
  <div class="card-header">
    <span class="category-badge"
          style="color:{accent}; background:{cat_bg}; border-color:{accent}40;">
      {cat_label}
    </span>
    <div class="query-meta-right">
      <span class="result-pill">{col_count} cols · {row_count} rows</span>
    </div>
  </div>

  <h2 class="query-title">
    <span class="query-num">Query {query['id']}</span>
    {escape(query['title'])}
  </h2>

  <p class="business-question">
    <span class="bq-label">Business Question</span>
    <em>{escape(query['business_question'])}</em>
  </p>

  <div class="sql-block-wrapper">
    <div class="sql-toolbar">
      <span class="sql-lang-tag">SQL · SQLite</span>
      <button class="copy-btn" onclick="copySQL(this)">Copy</button>
    </div>
    <pre class="language-sql"><code class="language-sql">{sql_escaped}</code></pre>
  </div>

  <div class="results-section">
    <h3 class="results-heading">Results</h3>
    {table_html}
  </div>

  {'<div class="chart-section"><h3 class="results-heading">Visualization</h3>' + chart_html + '</div>' if chart_html else ''}

  <div class="interpretation">
    <h3 class="interp-heading">Interpretation</h3>
    <p>{escape(query['interpretation'])}</p>
  </div>
</div>"""


# ---------------------------------------------------------------------------
# Page assembly
# ---------------------------------------------------------------------------

def build_html(query_cards_html, query_count):
    now = datetime.now().strftime("%B %d, %Y")

    filter_pills = [
        ("all",         "All"),
        ("window",      "Window Functions"),
        ("cte",         "CTEs"),
        ("aggregation", "Aggregation"),
        ("date",        "Date Analysis"),
        ("joins",       "JOINs"),
        ("statistical", "Statistical"),
    ]

    pills_html = "\n".join(
        f'<button class="filter-pill{" active" if k == "all" else ""}" '
        f'data-filter="{k}">{label}</button>'
        for k, label in filter_pills
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>SQL Analytics Showcase — TransactGuard AML</title>

  <!-- Fonts -->
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet" />

  <!-- Prism.js syntax highlighting -->
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" />

  <!-- Chart.js -->
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.2/dist/chart.umd.min.js"></script>

  <style>
    /* ── Reset & Base ─────────────────────────────────────────────────── */
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

    :root {{
      --bg:          #0d1117;
      --card-bg:     #161b22;
      --border:      #30363d;
      --text:        #c9d1d9;
      --text-muted:  #8b949e;
      --accent:      #58a6ff;
      --accent-dim:  #1f3a5c;
      --code-bg:     #1e2a3a;
      --radius:      10px;
      --radius-sm:   6px;
      --shadow:      0 4px 24px rgba(0,0,0,.45);
    }}

    html {{ scroll-behavior: smooth; }}

    body {{
      font-family: 'Inter', sans-serif;
      background: var(--bg);
      color: var(--text);
      line-height: 1.65;
      min-height: 100vh;
    }}

    a {{ color: var(--accent); text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}

    /* ── Top nav bar ─────────────────────────────────────────────────── */
    .top-nav {{
      background: var(--card-bg);
      border-bottom: 1px solid var(--border);
      padding: 0.75rem 2rem;
      display: flex;
      align-items: center;
      gap: 1rem;
      position: sticky;
      top: 0;
      z-index: 100;
      backdrop-filter: blur(8px);
    }}

    .nav-brand {{
      display: flex;
      align-items: center;
      gap: 0.5rem;
      font-weight: 600;
      font-size: 0.95rem;
      color: var(--text);
      letter-spacing: -0.01em;
    }}

    .nav-brand .brand-icon {{ font-size: 1.1rem; }}

    .nav-back {{
      margin-left: auto;
      font-size: 0.85rem;
      color: var(--text-muted);
      display: flex;
      align-items: center;
      gap: 0.35rem;
      transition: color .2s;
    }}
    .nav-back:hover {{ color: var(--accent); text-decoration: none; }}

    /* ── Hero header ────────────────────────────────────────────────── */
    .hero {{
      max-width: 1200px;
      margin: 3rem auto 0;
      padding: 0 2rem;
      text-align: center;
    }}

    .hero-eyebrow {{
      font-size: 0.75rem;
      font-weight: 600;
      letter-spacing: .12em;
      text-transform: uppercase;
      color: var(--accent);
      margin-bottom: 0.75rem;
    }}

    .hero h1 {{
      font-size: clamp(1.8rem, 4vw, 2.8rem);
      font-weight: 700;
      letter-spacing: -0.025em;
      color: #e6edf3;
      line-height: 1.2;
      margin-bottom: 1rem;
    }}

    .hero-subtitle {{
      font-size: 1.05rem;
      color: var(--text-muted);
      max-width: 680px;
      margin: 0 auto 2.5rem;
    }}

    /* ── Stats ribbon ────────────────────────────────────────────────── */
    .stats-ribbon {{
      display: flex;
      justify-content: center;
      gap: 2.5rem;
      flex-wrap: wrap;
      margin-bottom: 2.5rem;
    }}

    .stat-item {{
      text-align: center;
    }}

    .stat-num {{
      font-size: 1.9rem;
      font-weight: 700;
      color: var(--accent);
      letter-spacing: -0.03em;
    }}

    .stat-label {{
      font-size: 0.75rem;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: .07em;
      margin-top: 0.1rem;
    }}

    /* ── Filter pills ────────────────────────────────────────────────── */
    .filter-bar {{
      max-width: 1200px;
      margin: 0 auto 2.5rem;
      padding: 0 2rem;
      display: flex;
      gap: 0.5rem;
      flex-wrap: wrap;
      justify-content: center;
    }}

    .filter-pill {{
      padding: 0.45rem 1.1rem;
      border-radius: 99px;
      border: 1px solid var(--border);
      background: var(--card-bg);
      color: var(--text-muted);
      font-size: 0.82rem;
      font-weight: 500;
      cursor: pointer;
      transition: all .2s;
      font-family: 'Inter', sans-serif;
    }}

    .filter-pill:hover {{
      border-color: var(--accent);
      color: var(--accent);
    }}

    .filter-pill.active {{
      background: var(--accent);
      border-color: var(--accent);
      color: #0d1117;
      font-weight: 600;
    }}

    /* ── Cards grid ─────────────────────────────────────────────────── */
    .cards-grid {{
      max-width: 1200px;
      margin: 0 auto 5rem;
      padding: 0 2rem;
      display: grid;
      grid-template-columns: 1fr;
      gap: 2rem;
    }}

    /* ── Query card ──────────────────────────────────────────────────── */
    .query-card {{
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 2rem;
      box-shadow: var(--shadow);
      transition: border-color .25s, box-shadow .25s;
    }}

    .query-card:hover {{
      border-color: #444c56;
      box-shadow: 0 6px 32px rgba(0,0,0,.55);
    }}

    .query-card.hidden {{ display: none; }}

    /* card header row */
    .card-header {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 1.1rem;
      flex-wrap: wrap;
      gap: 0.5rem;
    }}

    .category-badge {{
      display: inline-flex;
      align-items: center;
      gap: 0.35rem;
      padding: 0.3rem 0.75rem;
      border-radius: 99px;
      border: 1px solid;
      font-size: 0.75rem;
      font-weight: 600;
      letter-spacing: .05em;
      text-transform: uppercase;
    }}

    .result-pill {{
      font-size: 0.72rem;
      color: var(--text-muted);
      background: #21262d;
      border: 1px solid var(--border);
      padding: 0.25rem 0.6rem;
      border-radius: 99px;
    }}

    /* title */
    .query-title {{
      font-size: 1.25rem;
      font-weight: 700;
      color: #e6edf3;
      margin-bottom: 0.75rem;
      letter-spacing: -0.015em;
    }}

    .query-num {{
      color: var(--accent);
      font-size: 0.85rem;
      font-weight: 500;
      display: block;
      margin-bottom: 0.2rem;
      letter-spacing: .04em;
      text-transform: uppercase;
    }}

    /* business question */
    .business-question {{
      display: flex;
      align-items: baseline;
      gap: 0.6rem;
      font-size: 0.93rem;
      margin-bottom: 1.5rem;
      padding: 0.75rem 1rem;
      background: #1c2128;
      border-left: 3px solid var(--accent);
      border-radius: 0 var(--radius-sm) var(--radius-sm) 0;
    }}

    .bq-label {{
      font-size: 0.7rem;
      font-weight: 700;
      letter-spacing: .07em;
      text-transform: uppercase;
      color: var(--accent);
      white-space: nowrap;
      flex-shrink: 0;
    }}

    .business-question em {{
      font-style: italic;
      color: var(--text);
    }}

    /* SQL block */
    .sql-block-wrapper {{
      border-radius: var(--radius-sm);
      overflow: hidden;
      margin-bottom: 1.75rem;
      border: 1px solid #253040;
    }}

    .sql-toolbar {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      background: #182230;
      padding: 0.5rem 1rem;
      border-bottom: 1px solid #253040;
    }}

    .sql-lang-tag {{
      font-size: 0.7rem;
      font-weight: 600;
      letter-spacing: .07em;
      text-transform: uppercase;
      color: #4a8ac4;
    }}

    .copy-btn {{
      font-size: 0.72rem;
      background: #1e2a3a;
      border: 1px solid #30363d;
      color: var(--text-muted);
      padding: 0.2rem 0.6rem;
      border-radius: 4px;
      cursor: pointer;
      font-family: 'Inter', sans-serif;
      transition: all .2s;
    }}

    .copy-btn:hover {{ color: var(--accent); border-color: var(--accent); }}
    .copy-btn.copied {{ color: #3fb950; border-color: #3fb950; }}

    /* Override Prism tomorrow theme background to match our code-bg */
    pre[class*="language-"] {{
      background: var(--code-bg) !important;
      margin: 0 !important;
      border-radius: 0 !important;
      font-family: 'JetBrains Mono', 'Fira Code', monospace !important;
      font-size: 0.82rem !important;
      line-height: 1.7 !important;
      padding: 1.25rem 1.5rem !important;
      overflow-x: auto;
    }}

    code[class*="language-"] {{
      font-family: 'JetBrains Mono', 'Fira Code', monospace !important;
      font-size: 0.82rem !important;
    }}

    /* ── Results table ───────────────────────────────────────────────── */
    .results-heading, .interp-heading {{
      font-size: 0.78rem;
      font-weight: 700;
      letter-spacing: .09em;
      text-transform: uppercase;
      color: var(--text-muted);
      margin-bottom: 0.75rem;
    }}

    .table-wrapper {{
      overflow-x: auto;
      border-radius: var(--radius-sm);
      border: 1px solid var(--border);
      margin-bottom: 0.5rem;
    }}

    .results-table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.82rem;
      white-space: nowrap;
    }}

    .results-table thead tr {{
      background: #1c2128;
    }}

    .results-table th {{
      padding: 0.6rem 1rem;
      text-align: left;
      font-weight: 600;
      color: var(--accent);
      border-bottom: 1px solid var(--border);
      letter-spacing: .04em;
      font-size: 0.75rem;
      text-transform: uppercase;
    }}

    .results-table td {{
      padding: 0.55rem 1rem;
      border-bottom: 1px solid #21262d;
      color: var(--text);
    }}

    .results-table tbody tr:last-child td {{
      border-bottom: none;
    }}

    .results-table tbody tr:hover td {{
      background: #1c2128;
    }}

    .results-table tbody tr:nth-child(even) td {{
      background: #0f1319;
    }}

    .results-table tbody tr:nth-child(even):hover td {{
      background: #1c2128;
    }}

    .row-note {{
      font-size: 0.75rem;
      color: var(--text-muted);
      margin-bottom: 1.5rem;
    }}

    .no-data {{
      font-size: 0.85rem;
      color: var(--text-muted);
      padding: 1rem 0;
      margin-bottom: 1.5rem;
    }}

    /* ── Chart ───────────────────────────────────────────────────────── */
    .chart-section {{
      margin-bottom: 1.75rem;
    }}

    .chart-container {{
      background: #0f1319;
      border: 1px solid var(--border);
      border-radius: var(--radius-sm);
      padding: 1.25rem;
      height: 260px;
    }}

    /* ── Interpretation ──────────────────────────────────────────────── */
    .interpretation {{
      padding: 1rem 1.25rem;
      background: #0f1319;
      border-radius: var(--radius-sm);
      border: 1px solid var(--border);
    }}

    .interpretation p {{
      font-size: 0.9rem;
      color: var(--text-muted);
      line-height: 1.7;
    }}

    /* ── Footer ──────────────────────────────────────────────────────── */
    footer {{
      text-align: center;
      padding: 2.5rem 2rem;
      border-top: 1px solid var(--border);
      color: var(--text-muted);
      font-size: 0.8rem;
    }}

    footer .footer-brand {{
      font-weight: 600;
      color: var(--accent);
    }}

    /* ── Scrollbar styling (webkit) ──────────────────────────────────── */
    ::-webkit-scrollbar {{ width: 6px; height: 6px; }}
    ::-webkit-scrollbar-track {{ background: var(--bg); }}
    ::-webkit-scrollbar-thumb {{ background: #444c56; border-radius: 3px; }}
    ::-webkit-scrollbar-thumb:hover {{ background: #6e7681; }}

    /* ── Responsive ──────────────────────────────────────────────────── */
    @media (max-width: 768px) {{
      .hero h1 {{ font-size: 1.6rem; }}
      .hero-subtitle {{ font-size: 0.9rem; }}
      .query-card {{ padding: 1.25rem; }}
      .stats-ribbon {{ gap: 1.5rem; }}
      .top-nav {{ padding: 0.75rem 1rem; }}
    }}
  </style>
</head>
<body>

<!-- Top nav -->
<nav class="top-nav">
  <div class="nav-brand">
    <span class="brand-icon">🗄️</span>
    TransactGuard AML
  </div>
  <a class="nav-back" href="executive_dashboard.html">
    ← Executive Dashboard
  </a>
</nav>

<!-- Hero -->
<header class="hero">
  <p class="hero-eyebrow">Portfolio · SQL Proficiency Showcase</p>
  <h1>🗄️ TransactGuard AML<br>SQL Analytics Showcase</h1>
  <p class="hero-subtitle">
    {query_count} analytical queries demonstrating advanced SQL proficiency across
    window functions, CTEs, aggregations, date analysis, JOINs, and statistical methods —
    all executed against a real AML transaction database.
  </p>

  <div class="stats-ribbon">
    <div class="stat-item">
      <div class="stat-num">{query_count}</div>
      <div class="stat-label">Queries</div>
    </div>
    <div class="stat-item">
      <div class="stat-num">6</div>
      <div class="stat-label">SQL Categories</div>
    </div>
    <div class="stat-item">
      <div class="stat-num">50k+</div>
      <div class="stat-label">Transactions Analyzed</div>
    </div>
    <div class="stat-item">
      <div class="stat-num">6</div>
      <div class="stat-label">Tables Queried</div>
    </div>
  </div>
</header>

<!-- Filter pills -->
<div class="filter-bar">
  {pills_html}
</div>

<!-- Query cards -->
<main class="cards-grid" id="cards-grid">
  {query_cards_html}
</main>

<footer>
  Generated {now} &nbsp;·&nbsp;
  <span class="footer-brand">TransactGuard AML</span> &nbsp;·&nbsp;
  SQL executed against SQLite 3 · real data, no mock results
</footer>

<!-- Prism.js -->
<script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>

<script>
/* ── Filter logic ──────────────────────────────────────────────────── */
(function () {{
  var pills   = document.querySelectorAll('.filter-pill');
  var cards   = document.querySelectorAll('.query-card');

  pills.forEach(function (pill) {{
    pill.addEventListener('click', function () {{
      var filter = this.dataset.filter;

      // Update active pill
      pills.forEach(function (p) {{ p.classList.remove('active'); }});
      this.classList.add('active');

      // Show/hide cards
      cards.forEach(function (card) {{
        if (filter === 'all' || card.dataset.category === filter) {{
          card.classList.remove('hidden');
        }} else {{
          card.classList.add('hidden');
        }}
      }});
    }});
  }});
}})();

/* ── Copy SQL button ───────────────────────────────────────────────── */
function copySQL(btn) {{
  var pre  = btn.closest('.sql-block-wrapper').querySelector('code');
  var text = pre ? pre.textContent : '';
  navigator.clipboard.writeText(text).then(function () {{
    btn.textContent = 'Copied!';
    btn.classList.add('copied');
    setTimeout(function () {{
      btn.textContent = 'Copy';
      btn.classList.remove('copied');
    }}, 2000);
  }}).catch(function () {{
    btn.textContent = 'Error';
  }});
}}
</script>

</body>
</html>"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"Connecting to {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    card_fragments = []
    for query in QUERIES:
        qid = query["id"]
        title = query["title"]
        print(f"  Running Query {qid}: {title} ...")
        sql = query.get("sql_sqlite", query["sql"])
        cols, rows = run_query(conn, sql)
        if cols:
            print(f"    -> {len(rows)} rows, cols: {cols}")
        card_html = render_query_card(query, cols, rows)
        card_fragments.append(card_html)

    conn.close()

    cards_html = "\n".join(card_fragments)
    page_html = build_html(cards_html, len(QUERIES))

    with open(OUTPUT_PATH, "w", encoding="utf-8") as fh:
        fh.write(page_html)

    size_kb = os.path.getsize(OUTPUT_PATH) / 1024
    print(f"\nWrote {OUTPUT_PATH}")
    print(f"File size: {size_kb:.1f} KB")
    if size_kb < 80:
        print("WARNING: file is smaller than expected (< 80 KB)")
    else:
        print("OK: file exceeds 80 KB threshold")


if __name__ == "__main__":
    main()
