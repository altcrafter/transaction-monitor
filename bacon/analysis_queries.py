"""
analysis_queries.py — 20+ advanced SQL queries demonstrating analytical techniques.

Each query is implemented as a function that runs the SQL, prints formatted
results, and returns the raw data. Demonstrates:
  JOINs, window functions, CTEs, subqueries, CASE, HAVING,
  date analysis, self-joins, statistical computations.

Run: python analysis_queries.py
"""

from __future__ import annotations

import sqlite3
from typing import Any

import db

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def run(conn: sqlite3.Connection, title: str, sql: str,
        params: tuple = ()) -> list[dict[str, Any]]:
    """Execute SQL, print a formatted table, return rows as dicts.

    Args:
        conn:   Open database connection.
        title:  Display title for this query.
        sql:    SQL query string.
        params: Query parameters.

    Returns:
        List of row dicts.
    """
    print(f"\n{'-'*70}")
    print(f"  {title}")
    print(f"{'-'*70}")
    rows = conn.execute(sql, params).fetchall()
    if not rows:
        print("  (no results)")
        return []
    cols = rows[0].keys()
    # Print header
    col_widths = {c: max(len(c), max(len(str(r[c])) for r in rows)) for c in cols}
    header = "  " + "  ".join(f"{c:<{col_widths[c]}}" for c in cols)
    print(header)
    print("  " + "-" * (len(header) - 2))
    for row in rows:
        line = "  " + "  ".join(f"{str(row[c]):<{col_widths[c]}}" for c in cols)
        print(line)
    print(f"  ({len(rows)} rows)")
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────────────────────────────────────
# Q1: Monthly transaction trends with MoM growth
# ─────────────────────────────────────────────────────────────────────────────

def q1_monthly_trends(conn: sqlite3.Connection) -> list:
    """Monthly transaction count and volume with month-over-month growth (window function)."""
    return run(conn, "Q1: Monthly Trends with MoM Growth (window function)", """
        WITH monthly AS (
            SELECT
                strftime('%Y-%m', timestamp) AS month,
                COUNT(*)                     AS txn_count,
                ROUND(SUM(te.amount_usd), 0) AS volume_usd
            FROM transactions t
            JOIN transaction_enrichment te ON te.transaction_id = t.id
            GROUP BY month
        )
        SELECT
            month,
            txn_count,
            volume_usd,
            txn_count - LAG(txn_count)  OVER (ORDER BY month) AS mom_count_delta,
            ROUND(100.0 * (txn_count - LAG(txn_count) OVER (ORDER BY month))
                  / LAG(txn_count) OVER (ORDER BY month), 1) AS mom_count_pct,
            ROUND(volume_usd / 1e6, 1) AS volume_millions
        FROM monthly
        ORDER BY month
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Q2: Customer risk segmentation
# ─────────────────────────────────────────────────────────────────────────────

def q2_customer_segmentation(conn: sqlite3.Connection) -> list:
    """Customer segmentation by risk rating with transaction and alert stats (multi-join + CASE)."""
    return run(conn, "Q2: Customer Risk Segmentation (multi-join, CASE)", """
        SELECT
            CASE c.risk_rating
                WHEN 1 THEN 'Low'
                WHEN 2 THEN 'Medium'
                WHEN 3 THEN 'High'
            END                              AS risk_segment,
            c.customer_type,
            COUNT(DISTINCT c.id)             AS customers,
            COUNT(DISTINCT t.id)             AS transactions,
            ROUND(AVG(te.amount_usd), 0)     AS avg_txn_usd,
            COUNT(DISTINCT al.id)            AS alerts,
            ROUND(100.0 * COUNT(DISTINCT al.id) / NULLIF(COUNT(DISTINCT t.id), 0), 2) AS alert_rate_pct
        FROM customers c
        LEFT JOIN accounts a   ON a.customer_id = c.id
        LEFT JOIN transactions t ON t.account_id = a.id
        LEFT JOIN transaction_enrichment te ON te.transaction_id = t.id
        LEFT JOIN alerts al    ON al.transaction_id = t.id
        GROUP BY c.risk_rating, c.customer_type
        ORDER BY c.risk_rating, c.customer_type
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Q3: Alert funnel — triage analysis
# ─────────────────────────────────────────────────────────────────────────────

def q3_alert_funnel(conn: sqlite3.Connection) -> list:
    """Alert score distribution funnel with cumulative percentages (CTE + window)."""
    return run(conn, "Q3: Alert Score Funnel (CTE + window functions)", """
        WITH bands AS (
            SELECT
                CASE
                    WHEN score >= 80 THEN '80-100 Critical'
                    WHEN score >= 60 THEN '60-79  High'
                    WHEN score >= 40 THEN '40-59  Medium'
                    WHEN score >= 20 THEN '20-39  Low'
                    ELSE                   '0-19   Minimal'
                END AS band,
                COUNT(*) n
            FROM alerts
            GROUP BY band
        )
        SELECT
            band,
            n,
            ROUND(100.0 * n / SUM(n) OVER (), 1) AS pct_of_total,
            SUM(n) OVER (ORDER BY band DESC)      AS cumulative_n,
            ROUND(100.0 * SUM(n) OVER (ORDER BY band DESC) / SUM(n) OVER (), 1) AS cumulative_pct
        FROM bands
        ORDER BY band DESC
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Q4: Rule precision analysis
# ─────────────────────────────────────────────────────────────────────────────

def q4_rule_precision(conn: sqlite3.Connection) -> list:
    """Rule performance with precision, recall, and F1-style metrics (subquery join)."""
    return run(conn, "Q4: Rule Precision Analysis (subquery join)", """
        SELECT
            r.name,
            r.category,
            r.severity,
            rp.total_alerts,
            rp.true_positives   AS tp,
            rp.false_positives  AS fp,
            ROUND(rp.precision_rate * 100, 1) AS precision_pct,
            rp.avg_score,
            rp.p95_score
        FROM rules r
        JOIN rule_performance rp ON rp.rule_id = r.id
        ORDER BY rp.precision_rate DESC
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Q5: Structuring detection — deposit pattern analysis
# ─────────────────────────────────────────────────────────────────────────────

def q5_structuring_detection(conn: sqlite3.Connection) -> list:
    """Identify accounts with repeated sub-threshold cash deposits (HAVING + aggregate)."""
    return run(conn, "Q5: Structuring Detection (HAVING + date grouping)", """
        SELECT
            t.account_id,
            c.name              AS customer_name,
            c.risk_rating,
            strftime('%Y-%m', t.timestamp) AS month,
            COUNT(*)            AS sub_threshold_deposits,
            ROUND(SUM(te.amount_usd), 2) AS total_usd,
            ROUND(AVG(te.amount_usd), 2) AS avg_usd,
            ROUND(MIN(te.amount_usd), 2) AS min_deposit,
            ROUND(MAX(te.amount_usd), 2) AS max_deposit
        FROM transactions t
        JOIN transaction_enrichment te ON te.transaction_id = t.id
        JOIN accounts a ON a.id = t.account_id
        JOIN customers c ON c.id = a.customer_id
        WHERE t.transaction_type = 'cash_deposit'
          AND te.amount_usd BETWEEN 9000 AND 9999
        GROUP BY t.account_id, month
        HAVING COUNT(*) >= 3
        ORDER BY sub_threshold_deposits DESC
        LIMIT 15
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Q6: Geographic flow analysis
# ─────────────────────────────────────────────────────────────────────────────

def q6_geographic_flows(conn: sqlite3.Connection) -> list:
    """Transaction flows by counterparty country with risk overlay (multi-join + CASE)."""
    return run(conn, "Q6: Geographic Flow Analysis (multi-join + CASE)", """
        SELECT
            COALESCE(t.counterparty_country, 'DOMESTIC') AS country,
            cr.country_name,
            CASE cr.risk_level
                WHEN 1 THEN 'Low'
                WHEN 2 THEN 'Medium'
                WHEN 3 THEN 'High'
                WHEN 4 THEN 'Very High'
                WHEN 5 THEN 'Sanctioned'
                ELSE 'Domestic'
            END AS risk_label,
            COUNT(t.id)                     AS txn_count,
            ROUND(SUM(te.amount_usd), 0)    AS total_usd,
            COUNT(DISTINCT t.account_id)    AS unique_accounts,
            COUNT(DISTINCT al.id)           AS alerts_generated
        FROM transactions t
        LEFT JOIN country_risk cr ON cr.country_code = t.counterparty_country
        JOIN transaction_enrichment te ON te.transaction_id = t.id
        LEFT JOIN alerts al ON al.transaction_id = t.id
        GROUP BY t.counterparty_country
        HAVING txn_count > 50
        ORDER BY total_usd DESC
        LIMIT 15
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Q7: Time-of-day heatmap
# ─────────────────────────────────────────────────────────────────────────────

def q7_time_heatmap(conn: sqlite3.Connection) -> list:
    """Transaction volume by day-of-week × hour-of-day (date functions + CASE)."""
    return run(conn, "Q7: Day × Hour Heatmap — top activity windows (date functions)", """
        WITH heatmap AS (
            SELECT
                CASE CAST(strftime('%w', timestamp) AS INTEGER)
                    WHEN 0 THEN 'Sunday'
                    WHEN 1 THEN 'Monday'
                    WHEN 2 THEN 'Tuesday'
                    WHEN 3 THEN 'Wednesday'
                    WHEN 4 THEN 'Thursday'
                    WHEN 5 THEN 'Friday'
                    WHEN 6 THEN 'Saturday'
                END                             AS day_name,
                CAST(strftime('%w', timestamp) AS INTEGER) AS dow,
                CAST(strftime('%H', timestamp) AS INTEGER) AS hour,
                COUNT(*) n,
                ROUND(SUM(te.amount_usd), 0) vol
            FROM transactions t
            JOIN transaction_enrichment te ON te.transaction_id = t.id
            GROUP BY dow, hour
        )
        SELECT day_name, hour, n AS txn_count, vol AS volume_usd,
               ROUND(100.0 * n / SUM(n) OVER (), 3) AS pct_of_all
        FROM heatmap
        ORDER BY n DESC
        LIMIT 15
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Q8: Z-score anomaly detection on transaction amounts
# ─────────────────────────────────────────────────────────────────────────────

def q8_zscore_anomalies(conn: sqlite3.Connection) -> list:
    """Transactions with z-score > 4 relative to their customer's normal amounts (CTE + stats)."""
    return run(conn, "Q8: Z-score Amount Anomalies (CTE + window stats)", """
        WITH customer_stats AS (
            SELECT
                c.id         AS customer_id,
                AVG(te.amount_usd)  AS mean_usd,
                -- SQLite has no STDEV; use variance formula manually
                SQRT(AVG(te.amount_usd * te.amount_usd) - AVG(te.amount_usd) * AVG(te.amount_usd)) AS stdev_usd,
                COUNT(t.id)  AS txn_count
            FROM customers c
            JOIN accounts a   ON a.customer_id = c.id
            JOIN transactions t ON t.account_id = a.id
            JOIN transaction_enrichment te ON te.transaction_id = t.id
            GROUP BY c.id
            HAVING COUNT(t.id) >= 10
        ),
        scored AS (
            SELECT
                t.id AS txn_id,
                c.id AS customer_id,
                c.name AS customer_name,
                te.amount_usd,
                cs.mean_usd,
                cs.stdev_usd,
                CASE WHEN cs.stdev_usd > 0
                    THEN (te.amount_usd - cs.mean_usd) / cs.stdev_usd
                    ELSE 0
                END AS z_score,
                t.transaction_type,
                t.timestamp
            FROM transactions t
            JOIN transaction_enrichment te ON te.transaction_id = t.id
            JOIN accounts a ON a.id = t.account_id
            JOIN customers c ON c.id = a.customer_id
            JOIN customer_stats cs ON cs.customer_id = c.id
        )
        SELECT
            customer_name, txn_id, transaction_type,
            ROUND(amount_usd, 0) AS amount_usd,
            ROUND(mean_usd, 0)   AS customer_avg,
            ROUND(z_score, 1)    AS z_score,
            timestamp
        FROM scored
        WHERE z_score > 4
        ORDER BY z_score DESC
        LIMIT 15
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Q9: Round-trip transfer detection (self-join)
# ─────────────────────────────────────────────────────────────────────────────

def q9_round_trip_detection(conn: sqlite3.Connection) -> list:
    """Find wire-out / wire-in pairs to same counterparty within 48h (self-join)."""
    return run(conn, "Q9: Round-Trip Transfer Detection (self-join)", """
        SELECT
            t_out.account_id,
            t_out.counterparty_account,
            ROUND(t_out.amount, 2) AS out_amount,
            ROUND(t_in.amount, 2)  AS in_amount,
            t_out.timestamp        AS out_time,
            t_in.timestamp         AS in_time,
            ROUND((julianday(t_in.timestamp) - julianday(t_out.timestamp)) * 24, 1) AS hours_elapsed,
            ROUND(ABS(t_out.amount - t_in.amount), 2) AS amount_diff
        FROM transactions t_out
        JOIN transactions t_in
          ON  t_in.account_id          = t_out.account_id
          AND t_in.counterparty_account = t_out.counterparty_account
          AND t_in.transaction_type    = 'wire_in'
          AND t_out.transaction_type   = 'wire_out'
          AND t_in.timestamp > t_out.timestamp
          AND julianday(t_in.timestamp) - julianday(t_out.timestamp) <= 2.0
        ORDER BY t_out.timestamp
        LIMIT 15
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Q10: Dormant account reactivation
# ─────────────────────────────────────────────────────────────────────────────

def q10_dormant_activation(conn: sqlite3.Connection) -> list:
    """Accounts with long gaps followed by sudden activity (window + date diff)."""
    return run(conn, "Q10: Dormant Account Reactivation (window lag)", """
        WITH txn_gaps AS (
            SELECT
                t.account_id,
                t.timestamp,
                t.amount,
                te.amount_usd,
                LAG(t.timestamp) OVER (PARTITION BY t.account_id ORDER BY t.timestamp) AS prev_ts,
                ROUND((julianday(t.timestamp) -
                       julianday(LAG(t.timestamp) OVER (PARTITION BY t.account_id ORDER BY t.timestamp)))
                      , 0) AS gap_days
            FROM transactions t
            JOIN transaction_enrichment te ON te.transaction_id = t.id
        )
        SELECT
            g.account_id,
            c.name AS customer_name,
            c.risk_rating,
            g.prev_ts AS last_active,
            g.timestamp AS reactivation_date,
            g.gap_days,
            ROUND(g.amount_usd, 0) AS reactivation_amount_usd
        FROM txn_gaps g
        JOIN accounts a ON a.id = g.account_id
        JOIN customers c ON c.id = a.customer_id
        WHERE g.gap_days >= 180
        ORDER BY g.gap_days DESC
        LIMIT 15
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Q11: Velocity comparison — fraud vs normal accounts
# ─────────────────────────────────────────────────────────────────────────────

def q11_velocity_comparison(conn: sqlite3.Connection) -> list:
    """Compare velocity distributions: accounts with VEL-001 alerts vs normal accounts."""
    return run(conn, "Q11: Velocity Comparison — alerted vs normal (subquery)", """
        WITH alerted_accounts AS (
            SELECT DISTINCT t.account_id
            FROM alerts al
            JOIN rules r ON r.id = al.rule_id AND r.name = 'VEL-001 Rapid transaction burst'
            JOIN transactions t ON t.id = al.transaction_id
        ),
        stats AS (
            SELECT
                CASE WHEN aa.account_id IS NOT NULL THEN 'Fraud (VEL-001)' ELSE 'Normal' END AS group_label,
                COUNT(DISTINCT t.account_id) AS accounts,
                COUNT(*) AS transactions,
                ROUND(AVG(te.velocity_1h), 2)  AS avg_vel_1h,
                ROUND(AVG(te.velocity_24h), 2) AS avg_vel_24h,
                MAX(te.velocity_1h)            AS max_vel_1h,
                ROUND(AVG(te.amount_usd), 0)   AS avg_amount_usd
            FROM transactions t
            JOIN transaction_enrichment te ON te.transaction_id = t.id
            LEFT JOIN alerted_accounts aa ON aa.account_id = t.account_id
            GROUP BY group_label
        )
        SELECT * FROM stats ORDER BY group_label
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Q12: Counterparty network analysis
# ─────────────────────────────────────────────────────────────────────────────

def q12_counterparty_analysis(conn: sqlite3.Connection) -> list:
    """Most-shared counterparty accounts (accounts used by multiple customers)."""
    return run(conn, "Q12: Shared Counterparty Analysis (GROUP BY + HAVING)", """
        SELECT
            t.counterparty_account,
            t.counterparty_name,
            COUNT(DISTINCT t.account_id)     AS sending_accounts,
            COUNT(DISTINCT a.customer_id)    AS distinct_customers,
            COUNT(t.id)                      AS total_txns,
            ROUND(SUM(te.amount_usd), 0)     AS total_usd,
            COUNT(DISTINCT al.id)            AS alerts
        FROM transactions t
        JOIN accounts a ON a.id = t.account_id
        JOIN transaction_enrichment te ON te.transaction_id = t.id
        LEFT JOIN alerts al ON al.transaction_id = t.id
        WHERE t.counterparty_account IS NOT NULL
        GROUP BY t.counterparty_account
        HAVING distinct_customers >= 2
        ORDER BY distinct_customers DESC, total_usd DESC
        LIMIT 15
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Q13: Currency exposure
# ─────────────────────────────────────────────────────────────────────────────

def q13_currency_exposure(conn: sqlite3.Connection) -> list:
    """Transaction volume by currency with USD equivalent and alert rate."""
    return run(conn, "Q13: Currency Exposure Analysis", """
        SELECT
            t.currency,
            COUNT(t.id)                      AS txn_count,
            ROUND(SUM(t.amount), 0)          AS total_native,
            ROUND(SUM(te.amount_usd), 0)     AS total_usd,
            ROUND(AVG(te.amount_usd), 0)     AS avg_usd,
            COUNT(DISTINCT al.id)            AS alerts,
            ROUND(100.0 * COUNT(DISTINCT al.id) / NULLIF(COUNT(t.id), 0), 2) AS alert_rate_pct
        FROM transactions t
        JOIN transaction_enrichment te ON te.transaction_id = t.id
        LEFT JOIN alerts al ON al.transaction_id = t.id
        GROUP BY t.currency
        ORDER BY total_usd DESC
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Q14: Seasonal pattern — weekday vs weekend
# ─────────────────────────────────────────────────────────────────────────────

def q14_seasonal_patterns(conn: sqlite3.Connection) -> list:
    """Weekday vs weekend transaction patterns with volume per-type breakdown."""
    return run(conn, "Q14: Seasonal Patterns — Weekday vs Weekend", """
        SELECT
            CASE CAST(strftime('%w', timestamp) AS INTEGER)
                WHEN 0 THEN 'Weekend'
                WHEN 6 THEN 'Weekend'
                ELSE 'Weekday'
            END AS day_type,
            transaction_type,
            COUNT(*) AS txn_count,
            ROUND(AVG(te.amount_usd), 0) AS avg_usd,
            ROUND(SUM(te.amount_usd), 0) AS total_usd
        FROM transactions t
        JOIN transaction_enrichment te ON te.transaction_id = t.id
        GROUP BY day_type, transaction_type
        ORDER BY day_type, txn_count DESC
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Q15: Rule co-occurrence — which rules fire together
# ─────────────────────────────────────────────────────────────────────────────

def q15_rule_cooccurrence(conn: sqlite3.Connection) -> list:
    """Rule pairs that frequently fire on the same transaction (self-join on alerts)."""
    return run(conn, "Q15: Rule Co-occurrence (self-join on alerts)", """
        SELECT
            r1.name AS rule_1,
            r2.name AS rule_2,
            COUNT(*) AS co_occurrences,
            ROUND(AVG(al1.score + al2.score) / 2, 1) AS avg_combined_score
        FROM alerts al1
        JOIN alerts al2 ON al2.transaction_id = al1.transaction_id
                       AND al2.rule_id > al1.rule_id
        JOIN rules r1 ON r1.id = al1.rule_id
        JOIN rules r2 ON r2.id = al2.rule_id
        GROUP BY al1.rule_id, al2.rule_id
        HAVING co_occurrences >= 5
        ORDER BY co_occurrences DESC
        LIMIT 15
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Q16: Customer lifetime value vs risk
# ─────────────────────────────────────────────────────────────────────────────

def q16_customer_lifetime(conn: sqlite3.Connection) -> list:
    """Customer tenure, volume, and risk — segmented by KYC and PEP status."""
    return run(conn, "Q16: Customer Lifetime Analysis (date math + CASE)", """
        SELECT
            c.customer_type,
            c.kyc_status,
            CASE c.pep_status WHEN 1 THEN 'PEP' ELSE 'Non-PEP' END AS pep_label,
            COUNT(DISTINCT c.id) AS customers,
            ROUND(AVG(
                (julianday('2024-12-31') - julianday(c.registration_date)) / 365.0
            ), 1) AS avg_tenure_years,
            ROUND(AVG(cust_vol.total_usd), 0) AS avg_lifetime_volume_usd,
            ROUND(AVG(cust_vol.total_alerts), 1) AS avg_alerts
        FROM customers c
        JOIN (
            SELECT a.customer_id,
                   SUM(te.amount_usd) AS total_usd,
                   COUNT(DISTINCT al.id) AS total_alerts
            FROM accounts a
            LEFT JOIN transactions t ON t.account_id = a.id
            LEFT JOIN transaction_enrichment te ON te.transaction_id = t.id
            LEFT JOIN alerts al ON al.transaction_id = t.id
            GROUP BY a.customer_id
        ) cust_vol ON cust_vol.customer_id = c.id
        GROUP BY c.customer_type, c.kyc_status, pep_label
        ORDER BY c.customer_type, c.kyc_status
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Q17: Threshold sensitivity analysis
# ─────────────────────────────────────────────────────────────────────────────

def q17_threshold_sensitivity(conn: sqlite3.Connection) -> list:
    """How many more alerts would be generated at different score thresholds?"""
    return run(conn, "Q17: Threshold Sensitivity Analysis (CASE)", """
        SELECT
            CASE
                WHEN score >= 70 THEN '>= 70'
                WHEN score >= 60 THEN '>= 60'
                WHEN score >= 50 THEN '>= 50'
                WHEN score >= 40 THEN '>= 40'
                WHEN score >= 30 THEN '>= 30'
                ELSE '< 30'
            END AS threshold,
            COUNT(*) AS alerts_at_or_above,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct,
            COUNT(DISTINCT transaction_id) AS unique_transactions
        FROM alerts
        GROUP BY threshold
        ORDER BY threshold DESC
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Q18: New customer risk — first 90 days
# ─────────────────────────────────────────────────────────────────────────────

def q18_new_customer_risk(conn: sqlite3.Connection) -> list:
    """Risk profile of transactions in a customer's first 90 days vs later (CTE + CASE)."""
    return run(conn, "Q18: New Customer Risk — First 90 Days vs Later (CTE + date diff)", """
        WITH customer_age AS (
            SELECT
                t.id AS txn_id,
                te.amount_usd,
                CASE
                    WHEN te.account_age_days <= 90 THEN 'First 90 days'
                    ELSE 'After 90 days'
                END AS tenure_phase
            FROM transactions t
            JOIN transaction_enrichment te ON te.transaction_id = t.id
        )
        SELECT
            tenure_phase,
            COUNT(*) AS txn_count,
            ROUND(AVG(ca.amount_usd), 0) AS avg_amount_usd,
            COUNT(DISTINCT al.id) AS alerts,
            ROUND(100.0 * COUNT(DISTINCT al.id) / NULLIF(COUNT(*), 0), 2) AS alert_rate_pct,
            ROUND(AVG(al.score), 1) AS avg_alert_score
        FROM customer_age ca
        LEFT JOIN alerts al ON al.transaction_id = ca.txn_id
        GROUP BY tenure_phase
        ORDER BY tenure_phase
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Q19: Alert resolution time analysis
# ─────────────────────────────────────────────────────────────────────────────

def q19_resolution_time(conn: sqlite3.Connection) -> list:
    """Distribution of alert resolution times (NULL = still open) by category."""
    return run(conn, "Q19: Alert Resolution Time by Category", """
        SELECT
            r.category,
            COUNT(al.id) AS total_alerts,
            SUM(CASE WHEN al.status = 'open' THEN 1 ELSE 0 END) AS still_open,
            ROUND(100.0 * SUM(CASE WHEN al.status = 'open' THEN 1 ELSE 0 END) / COUNT(al.id), 1) AS pct_open,
            ROUND(AVG(al.score), 1) AS avg_score,
            SUM(CASE WHEN al.score >= 60 THEN 1 ELSE 0 END) AS high_priority
        FROM alerts al
        JOIN rules r ON r.id = al.rule_id
        GROUP BY r.category
        ORDER BY total_alerts DESC
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Q20: Network cluster detection — multi-hop counterparty graph
# ─────────────────────────────────────────────────────────────────────────────

def q20_network_clusters(conn: sqlite3.Connection) -> list:
    """Accounts connected through shared counterparties — 2-hop network (CTE chain)."""
    return run(conn, "Q20: 2-hop Counterparty Network Clusters (recursive-style CTE)", """
        WITH
        -- Accounts that sent to the same counterparty (direct link)
        direct_links AS (
            SELECT
                t1.account_id AS acct_a,
                t2.account_id AS acct_b,
                t1.counterparty_account AS shared_counterparty,
                COUNT(*) AS shared_txns
            FROM transactions t1
            JOIN transactions t2
              ON  t2.counterparty_account = t1.counterparty_account
              AND t2.account_id > t1.account_id
            WHERE t1.counterparty_account IS NOT NULL
            GROUP BY t1.account_id, t2.account_id, t1.counterparty_account
        ),
        -- Count cluster size: how many accounts share each counterparty
        cluster_sizes AS (
            SELECT shared_counterparty,
                   COUNT(DISTINCT acct_a) + 1 AS cluster_size,
                   SUM(shared_txns) AS total_shared_txns
            FROM direct_links
            GROUP BY shared_counterparty
            HAVING cluster_size >= 3
        )
        SELECT
            cs.shared_counterparty,
            cs.cluster_size AS accounts_in_cluster,
            cs.total_shared_txns,
            -- Check if any cluster account has alerts
            (SELECT COUNT(DISTINCT al.id)
             FROM direct_links dl
             JOIN alerts al ON al.transaction_id IN (
                 SELECT id FROM transactions
                 WHERE account_id IN (dl.acct_a, dl.acct_b)
             )
             WHERE dl.shared_counterparty = cs.shared_counterparty
            ) AS cluster_alerts
        FROM cluster_sizes cs
        ORDER BY cluster_size DESC, total_shared_txns DESC
        LIMIT 10
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Q21: High-risk customer × high-risk country co-occurrence
# ─────────────────────────────────────────────────────────────────────────────

def q21_high_risk_matrix(conn: sqlite3.Connection) -> list:
    """Matrix of customer risk × counterparty risk country with alert density."""
    return run(conn, "Q21: Risk Matrix — Customer Risk × Country Risk", """
        SELECT
            CASE c.risk_rating WHEN 1 THEN 'Low' WHEN 2 THEN 'Med' WHEN 3 THEN 'High' END AS cust_risk,
            CASE te.country_risk_score
                WHEN 1 THEN 'Low'
                WHEN 2 THEN 'Med'
                WHEN 3 THEN 'High'
                WHEN 4 THEN 'V.High'
                WHEN 5 THEN 'Sanctnd'
            END AS country_risk,
            COUNT(t.id) AS transactions,
            COUNT(DISTINCT al.id) AS alerts,
            ROUND(100.0 * COUNT(DISTINCT al.id) / NULLIF(COUNT(t.id), 0), 1) AS alert_rate_pct,
            ROUND(AVG(al.score), 1) AS avg_score
        FROM transactions t
        JOIN transaction_enrichment te ON te.transaction_id = t.id
        JOIN accounts a ON a.id = t.account_id
        JOIN customers c ON c.id = a.customer_id
        LEFT JOIN alerts al ON al.transaction_id = t.id
        WHERE te.country_risk_score >= 2
        GROUP BY c.risk_rating, te.country_risk_score
        ORDER BY c.risk_rating, te.country_risk_score
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Q22: Top alerted customers with layering indicators
# ─────────────────────────────────────────────────────────────────────────────

def q22_top_customers(conn: sqlite3.Connection) -> list:
    """Top 15 customers by alert score with multi-rule flag (subquery EXISTS)."""
    return run(conn, "Q22: Top Alerted Customers with Multi-Rule Flag", """
        SELECT
            c.id,
            c.name,
            c.customer_type,
            c.risk_rating,
            c.country,
            COUNT(DISTINCT al.id) AS total_alerts,
            ROUND(MAX(al.score), 1) AS max_score,
            ROUND(AVG(al.score), 1) AS avg_score,
            COUNT(DISTINCT r.category) AS categories_triggered,
            CASE WHEN COUNT(DISTINCT r.category) >= 3 THEN 'YES' ELSE 'no' END AS multi_category_flag
        FROM customers c
        JOIN accounts a ON a.customer_id = c.id
        JOIN transactions t ON t.account_id = a.id
        JOIN alerts al ON al.transaction_id = t.id
        JOIN rules r ON r.id = al.rule_id
        GROUP BY c.id
        ORDER BY max_score DESC, total_alerts DESC
        LIMIT 15
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Q23: False positive analysis — low-risk customers with alerts
# ─────────────────────────────────────────────────────────────────────────────

def q23_false_positive_analysis(conn: sqlite3.Connection) -> list:
    """Low-risk customers (risk=1, verified KYC, non-PEP) receiving alerts — likely FPs."""
    return run(conn, "Q23: False Positive Analysis — Low-Risk Customers with Alerts", """
        SELECT
            r.name AS rule,
            r.category,
            COUNT(DISTINCT al.id) AS alerts_on_low_risk,
            COUNT(DISTINCT c.id) AS low_risk_customers,
            ROUND(AVG(al.score), 1) AS avg_score,
            ROUND(100.0 * COUNT(DISTINCT al.id) / NULLIF(
                (SELECT COUNT(*) FROM alerts WHERE rule_id = r.id), 0
            ), 1) AS pct_of_rule_alerts
        FROM alerts al
        JOIN rules r ON r.id = al.rule_id
        JOIN transactions t ON t.id = al.transaction_id
        JOIN accounts a ON a.id = t.account_id
        JOIN customers c ON c.id = a.customer_id
        WHERE c.risk_rating = 1
          AND c.kyc_status = 'verified'
          AND c.pep_status = 0
        GROUP BY r.id
        ORDER BY alerts_on_low_risk DESC
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Main — run all queries
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    """Run all analysis queries and print results."""
    print("=" * 70)
    print("  Transaction Monitor — Analysis Queries (23 queries)")
    print("=" * 70)

    conn = db.get_connection()
    try:
        q1_monthly_trends(conn)
        q2_customer_segmentation(conn)
        q3_alert_funnel(conn)
        q4_rule_precision(conn)
        q5_structuring_detection(conn)
        q6_geographic_flows(conn)
        q7_time_heatmap(conn)
        q8_zscore_anomalies(conn)
        q9_round_trip_detection(conn)
        q10_dormant_activation(conn)
        q11_velocity_comparison(conn)
        q12_counterparty_analysis(conn)
        q13_currency_exposure(conn)
        q14_seasonal_patterns(conn)
        q15_rule_cooccurrence(conn)
        q16_customer_lifetime(conn)
        q17_threshold_sensitivity(conn)
        q18_new_customer_risk(conn)
        q19_resolution_time(conn)
        q20_network_clusters(conn)
        q21_high_risk_matrix(conn)
        q22_top_customers(conn)
        q23_false_positive_analysis(conn)

        print(f"\n{'='*70}")
        print("  All 23 queries completed successfully.")
        print("=" * 70)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
