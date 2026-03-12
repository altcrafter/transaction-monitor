# Transaction Monitoring Platform — Dashboards, Reports & Portfolio

## Project Purpose
Portfolio project for a junior data analyst / compliance analyst job search.
Builder has strong math (through abstract algebra) and Python skills (complex Balatro modding).

**VISUAL QUALITY IS EVERYTHING.** Every chart, dashboard, and report should look professional enough to screenshot for a resume. DO NOT RUSH — iterate on visuals until they look genuinely impressive.

---

## Tech Constraints
- Chart.js and Prism.js from CDN only
- No npm, no build tools
- Embed all data as JSON in HTML files
- Python 3.10+
- SQLite backend

---

## Phases

### PHASE 0 — BOOTSTRAP (10 min)
Create simplified SQLite backend: customers, accounts, transactions (50k over 12 months), alerts (2000, realistic status distribution: 40% new, 25% under_review, 15% escalated, 10% true_positive, 10% false_positive), rules (15 with performance metrics), country_risk. Include basic fraud patterns. Just enough data to make dashboards look good.

### PHASE 1 — EXECUTIVE DASHBOARD (30+ min)
Generate `output/executive_dashboard.html`. Dark professional theme (navy/charcoal background). Components: header with platform name and date range; KPI cards row (total transactions with sparkline, active alerts with severity mini-bar, alert closure rate with trend, avg resolution time, total flagged amount, high-risk customer count); Chart row 1 (transaction volume line chart with alert overlay, alert severity donut); Chart row 2 (top 10 rules horizontal bar, geographic risk bar chart); Chart row 3 (alert status funnel, rule precision vs recall scatter); Recent alerts table (last 20, sortable, severity color coded). Use Chart.js from CDN. Embed data as JSON. Iterate 2-3 times on visual quality.

### PHASE 2 — ANALYST DASHBOARD (30+ min)
Generate `output/analyst_dashboard.html`. Interactive investigation tool. Alert queue panel (filterable by status/severity/date/rule, clickable cards). Alert detail panel (transaction details, customer profile, 30-day activity timeline chart, related alerts, rule explanation, similar transactions). Investigation tools (transaction search, network visualization showing customer-counterparty flows as SVG circles and lines, risk score breakdown stacked bar). All interactive with JS filtering and show/hide panels. Data embedded as JSON.

### PHASE 3 — AUTOMATED REPORTS (25+ min)
Generate three HTML reports:
1. `output/monthly_sar_summary.html` — Monthly Suspicious Activity Report with exec summary, statistics, category breakdown, top 5 alerts, trends, recommendations
2. `output/rule_effectiveness_report.html` — rule performance analysis with precision/recall, rankings, tuning recommendations, coverage analysis
3. `output/customer_risk_report.html` — high-risk customer list, risk distribution histogram, EDD requirements, KYC status

All reports: professional header with CONFIDENTIAL classification, table of contents, numbered sections, inline charts, Google Fonts, print-friendly CSS.

### PHASE 4 — DATA QUALITY MONITOR (20+ min)
Generate `output/data_quality_dashboard.html`. Green/yellow/red indicators for: completeness (NULL rates, missing enrichment), consistency (zero amounts, future dates, closed account transactions), timeliness (data freshness, ETL lag), statistical (volume anomalies, distribution shifts, new countries). Trend charts where relevant.

### PHASE 5 — SQL PLAYGROUND (20+ min)
Generate `output/sql_playground.html`. Curated showcase of 15+ analytical SQL queries. For each: title, business question, syntax-highlighted SQL (Prism.js from CDN), results table, visualization, 2-3 sentence interpretation. Categories: window functions, CTEs, aggregation, subqueries, CASE, date analysis, multi-table JOINs, self-joins, statistical computations. This demonstrates SQL proficiency to employers.

### PHASE 6 — PORTFOLIO LANDING PAGE (20+ min)
Generate `output/index.html`. Visually impressive landing page: hero section, project description, card grid linking to all outputs with thumbnails, tech stack section, architecture diagram (SVG: Raw Data -> ETL -> Database -> Rules -> Dashboards), key metrics section. Modern startup landing page aesthetic, not academic.

### PHASE 7 — FINAL POLISH (remaining time)
Review every HTML file for broken layouts and ugly sections. Add consistent navigation across all pages. Create `build.py` that runs full pipeline from clean state. Write `README.md`. Make final visual improvements to every output.

---

## Commit after each phase.
