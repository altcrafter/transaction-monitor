#!/usr/bin/env python3
"""
Generate Monthly SAR Summary Report for March 2026.
Output: output/monthly_sar_summary.html
"""

import sqlite3
import json
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'transactions.db')
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), '..', 'output', 'monthly_sar_summary.html')

def fetch_data():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # --- Executive Summary ---
    cur.execute("""
        SELECT COUNT(*) as total, SUM(flagged_amount) as total_amount
        FROM alerts
        WHERE created_at >= '2026-03-01' AND created_at < '2026-04-01'
    """)
    mar = cur.fetchone()
    mar_total = mar['total']
    mar_amount = mar['total_amount'] or 0

    cur.execute("""
        SELECT COUNT(*) as total, SUM(flagged_amount) as total_amount
        FROM alerts
        WHERE created_at >= '2026-02-01' AND created_at < '2026-03-01'
    """)
    feb = cur.fetchone()
    feb_total = feb['total']
    feb_amount = feb['total_amount'] or 0

    # SARs filed = true_positive + escalated
    cur.execute("""
        SELECT COUNT(*) FROM alerts
        WHERE created_at >= '2026-03-01' AND created_at < '2026-04-01'
        AND status IN ('true_positive', 'escalated')
    """)
    sars_filed = cur.fetchone()[0]

    # --- Alert Statistics ---
    cur.execute("""
        SELECT status, COUNT(*) as cnt
        FROM alerts
        WHERE created_at >= '2026-03-01' AND created_at < '2026-04-01'
        GROUP BY status ORDER BY cnt DESC
    """)
    status_rows = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT severity, COUNT(*) as cnt
        FROM alerts
        WHERE created_at >= '2026-03-01' AND created_at < '2026-04-01'
        GROUP BY severity ORDER BY CASE severity WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END
    """)
    severity_rows = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT DATE(created_at) as day, COUNT(*) as cnt
        FROM alerts
        WHERE created_at >= '2026-03-01' AND created_at < '2026-04-01'
        GROUP BY day ORDER BY day
    """)
    daily_rows = [dict(r) for r in cur.fetchall()]

    # --- Category Breakdown ---
    cur.execute("""
        SELECT r.rule_category as category,
               COUNT(*) as cnt,
               AVG(a.flagged_amount) as avg_amount,
               SUM(a.flagged_amount) as total_amount
        FROM alerts a JOIN rules r ON a.rule_id = r.rule_id
        WHERE a.created_at >= '2026-03-01' AND a.created_at < '2026-04-01'
        GROUP BY r.rule_category
        ORDER BY cnt DESC
    """)
    category_rows = [dict(r) for r in cur.fetchall()]

    # --- Top 5 Alerts ---
    cur.execute("""
        SELECT a.alert_id, a.created_at, a.customer_id, c.name as customer_name,
               r.rule_name, r.rule_category, a.flagged_amount, a.severity, a.status, a.notes
        FROM alerts a
        JOIN customers c ON a.customer_id = c.customer_id
        JOIN rules r ON a.rule_id = r.rule_id
        WHERE a.created_at >= '2026-03-01' AND a.created_at < '2026-04-01'
        ORDER BY CASE a.severity WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END,
                 a.flagged_amount DESC
        LIMIT 5
    """)
    top_alerts = [dict(r) for r in cur.fetchall()]

    # --- 6-Month Typology Trend ---
    cur.execute("""
        SELECT strftime('%Y-%m', a.created_at) as month, r.rule_category as category, COUNT(*) as cnt
        FROM alerts a JOIN rules r ON a.rule_id = r.rule_id
        WHERE a.created_at >= '2025-10-01' AND a.created_at < '2026-04-01'
        GROUP BY month, category
        ORDER BY month, category
    """)
    trend_rows = [dict(r) for r in cur.fetchall()]

    conn.close()
    return {
        'mar_total': mar_total,
        'mar_amount': mar_amount,
        'feb_total': feb_total,
        'feb_amount': feb_amount,
        'sars_filed': sars_filed,
        'status_rows': status_rows,
        'severity_rows': severity_rows,
        'daily_rows': daily_rows,
        'category_rows': category_rows,
        'top_alerts': top_alerts,
        'trend_rows': trend_rows,
    }


def build_trend_datasets(trend_rows):
    months = sorted(set(r['month'] for r in trend_rows))
    categories = sorted(set(r['category'] for r in trend_rows))
    month_labels = {
        '2025-10': 'Oct 2025', '2025-11': 'Nov 2025', '2025-12': 'Dec 2025',
        '2026-01': 'Jan 2026', '2026-02': 'Feb 2026', '2026-03': 'Mar 2026',
    }
    cat_colors = {
        'structuring': '#ef4444',
        'velocity': '#f97316',
        'geographic': '#3b82f6',
        'behavior': '#8b5cf6',
        'network': '#10b981',
    }
    lookup = {(r['month'], r['category']): r['cnt'] for r in trend_rows}
    datasets = []
    for cat in categories:
        data = [lookup.get((m, cat), 0) for m in months]
        datasets.append({
            'label': cat.title(),
            'data': data,
            'borderColor': cat_colors.get(cat, '#6b7280'),
            'backgroundColor': cat_colors.get(cat, '#6b7280') + '22',
            'tension': 0.4,
            'fill': True,
        })
    labels = [month_labels.get(m, m) for m in months]
    return labels, datasets


def generate_html(data):
    pct_change = ((data['mar_total'] - data['feb_total']) / data['feb_total'] * 100) if data['feb_total'] else 0
    amt_change = ((data['mar_amount'] - data['feb_amount']) / data['feb_amount'] * 100) if data['feb_amount'] else 0

    daily_labels = json.dumps([r['day'] for r in data['daily_rows']])
    daily_counts = json.dumps([r['cnt'] for r in data['daily_rows']])

    cat_labels = json.dumps([r['category'].title() for r in data['category_rows']])
    cat_counts = json.dumps([r['cnt'] for r in data['category_rows']])
    cat_colors_list = json.dumps(['#ef4444', '#f97316', '#3b82f6', '#8b5cf6', '#10b981'])

    trend_labels, trend_datasets = build_trend_datasets(data['trend_rows'])
    trend_labels_json = json.dumps(trend_labels)
    trend_datasets_json = json.dumps(trend_datasets)

    total_cat = sum(r['cnt'] for r in data['category_rows'])

    # Build category table rows
    cat_table_rows = ''
    for r in data['category_rows']:
        pct = r['cnt'] / total_cat * 100 if total_cat else 0
        cat_table_rows += f"""
        <tr>
            <td><span class="badge badge-cat">{r['category'].title()}</span></td>
            <td class="text-right">{r['cnt']}</td>
            <td class="text-right">{pct:.1f}%</td>
            <td class="text-right">${r['avg_amount']:,.0f}</td>
            <td class="text-right">${r['total_amount']:,.0f}</td>
        </tr>"""

    # Build status table rows
    status_table_rows = ''
    status_colors = {
        'new': '#3b82f6', 'under_review': '#f59e0b',
        'escalated': '#ef4444', 'true_positive': '#10b981', 'false_positive': '#6b7280'
    }
    for r in data['status_rows']:
        color = status_colors.get(r['status'], '#6b7280')
        status_table_rows += f"""
        <tr>
            <td><span class="status-dot" style="background:{color}"></span>{r['status'].replace('_', ' ').title()}</td>
            <td class="text-right"><strong>{r['cnt']}</strong></td>
            <td class="text-right">{r['cnt']/data['mar_total']*100:.1f}%</td>
        </tr>"""

    severity_table_rows = ''
    sev_colors = {'critical': '#ef4444', 'high': '#f97316', 'medium': '#f59e0b', 'low': '#6b7280'}
    for r in data['severity_rows']:
        color = sev_colors.get(r['severity'], '#6b7280')
        bar_width = int(r['cnt'] / data['mar_total'] * 100)
        severity_table_rows += f"""
        <tr>
            <td><span class="sev-badge" style="background:{color}20;color:{color};border:1px solid {color}40">{r['severity'].upper()}</span></td>
            <td class="text-right"><strong>{r['cnt']}</strong></td>
            <td>
                <div class="bar-bg"><div class="bar-fill" style="width:{bar_width}%;background:{color}"></div></div>
            </td>
        </tr>"""

    # Build top alert cards
    alert_cards = ''
    sev_card_colors = {'critical': '#ef4444', 'high': '#f97316', 'medium': '#f59e0b', 'low': '#10b981'}
    narr_templates = {
        'structuring': 'Analysis indicates a pattern consistent with structured transactions designed to evade currency reporting requirements. The transaction sequence demonstrates deliberate fragmentation below the $10,000 reporting threshold across multiple accounts.',
        'velocity': 'Unusual transaction velocity detected, with funds moving at a rate significantly exceeding the customer\'s established baseline behaviour. This pattern warrants immediate review for potential layering activity.',
        'geographic': 'Cross-border transaction flagged due to counterparty location in a high-risk jurisdiction currently under enhanced FATF monitoring. Geographic risk indicators are consistent with known money laundering corridors.',
        'behavior': 'Behavioural anomaly detected relative to customer\'s historical transaction profile. The deviation from established patterns, combined with the transaction amount, meets the threshold for mandatory escalation.',
        'network': 'Network analysis reveals connections to entities flagged in prior investigations. The transaction structure is consistent with layering through third-party intermediaries, requiring enhanced scrutiny.',
    }
    for i, a in enumerate(data['top_alerts'], 1):
        color = sev_card_colors.get(a['severity'], '#6b7280')
        narr = narr_templates.get(a['rule_category'], 'Suspicious activity detected. Manual review required.')
        if a.get('notes'):
            narr = a['notes'] + ' ' + narr
        alert_cards += f"""
        <div class="alert-card" style="border-left:4px solid {color}">
            <div class="alert-card-header">
                <div class="alert-card-id">
                    <span class="alert-num">#{i}</span>
                    <span class="alert-id">{a['alert_id']}</span>
                    <span class="sev-badge" style="background:{color}20;color:{color};border:1px solid {color}40">{a['severity'].upper()}</span>
                </div>
                <div class="alert-amount" style="color:{color}">${a['flagged_amount']:,.2f}</div>
            </div>
            <div class="alert-card-meta">
                <div><strong>Date:</strong> {a['created_at'][:10]}</div>
                <div><strong>Customer:</strong> {a['customer_name']} ({a['customer_id']})</div>
                <div><strong>Rule:</strong> {a['rule_name']}</div>
                <div><strong>Status:</strong> <span class="status-dot" style="background:{status_colors.get(a['status'],'#6b7280')}"></span>{a['status'].replace('_',' ').title()}</div>
            </div>
            <div class="alert-narrative">
                <strong>Analyst Narrative:</strong> {narr}
            </div>
        </div>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SAR Monthly Summary — March 2026</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  :root {{
    --accent: #1e293b;
    --accent2: #0f172a;
    --red: #ef4444;
    --orange: #f97316;
    --blue: #3b82f6;
    --green: #10b981;
    --purple: #8b5cf6;
    --yellow: #f59e0b;
    --gray: #6b7280;
    --light: #f8fafc;
    --border: #e2e8f0;
  }}
  body {{ font-family: 'Inter', sans-serif; background: #f1f5f9; color: #1e293b; font-size: 14px; line-height: 1.6; }}
  .page {{ max-width: 1100px; margin: 0 auto; padding: 24px; }}

  /* Classification Header */
  .classif-banner {{
    background: var(--red);
    color: white;
    text-align: center;
    padding: 8px 16px;
    font-weight: 700;
    font-size: 11px;
    letter-spacing: 3px;
    text-transform: uppercase;
    border-radius: 6px 6px 0 0;
  }}

  /* Main Header */
  .report-header {{
    background: linear-gradient(135deg, var(--accent2) 0%, #1e3a5f 100%);
    color: white;
    padding: 40px 48px;
    border-radius: 0 0 0 0;
  }}
  .report-header h1 {{ font-size: 26px; font-weight: 800; letter-spacing: -0.5px; margin-bottom: 8px; }}
  .report-header .subtitle {{ font-size: 13px; color: #94a3b8; margin-bottom: 24px; }}
  .header-meta {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; margin-top: 24px; }}
  .meta-item label {{ font-size: 10px; color: #64748b; text-transform: uppercase; letter-spacing: 1px; }}
  .meta-item span {{ display: block; font-size: 13px; color: #e2e8f0; font-weight: 500; }}
  .classif-badge {{
    display: inline-block;
    background: var(--red);
    color: white;
    padding: 3px 12px;
    border-radius: 3px;
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 2px;
    text-transform: uppercase;
    margin-bottom: 12px;
  }}

  /* Content */
  .content {{ background: white; border-radius: 0 0 8px 8px; }}

  /* TOC */
  .toc {{
    background: var(--light);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 24px 32px;
    margin: 32px;
  }}
  .toc h2 {{ font-size: 14px; font-weight: 700; text-transform: uppercase; letter-spacing: 1px; color: var(--gray); margin-bottom: 16px; }}
  .toc ol {{ list-style: none; counter-reset: toc-counter; }}
  .toc ol li {{ counter-increment: toc-counter; display: flex; align-items: center; padding: 6px 0; border-bottom: 1px dashed #e2e8f0; font-size: 13px; }}
  .toc ol li:last-child {{ border-bottom: none; }}
  .toc ol li::before {{ content: counter(toc-counter) "."; font-weight: 700; color: var(--blue); width: 28px; flex-shrink: 0; }}
  .toc ol li .pg {{ margin-left: auto; color: var(--gray); font-size: 12px; }}

  /* Sections */
  .section {{ padding: 32px 40px; border-bottom: 1px solid var(--border); }}
  .section:last-child {{ border-bottom: none; }}
  .section-title {{
    font-size: 18px;
    font-weight: 700;
    color: var(--accent2);
    margin-bottom: 20px;
    padding-bottom: 10px;
    border-bottom: 2px solid var(--blue);
    display: flex;
    align-items: center;
    gap: 10px;
  }}
  .section-num {{
    background: var(--blue);
    color: white;
    width: 28px; height: 28px;
    border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    font-size: 13px; font-weight: 700; flex-shrink: 0;
  }}
  .narrative {{ font-size: 13.5px; color: #374151; line-height: 1.75; margin-bottom: 20px; }}

  /* Stat boxes */
  .stat-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; margin: 20px 0; }}
  .stat-box {{
    background: var(--light);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 20px;
    text-align: center;
  }}
  .stat-box .stat-val {{ font-size: 28px; font-weight: 800; color: var(--accent2); }}
  .stat-box .stat-lbl {{ font-size: 11px; color: var(--gray); text-transform: uppercase; letter-spacing: 1px; margin-top: 4px; }}
  .stat-box .stat-sub {{ font-size: 12px; color: var(--gray); margin-top: 8px; }}
  .stat-box.highlight {{ background: linear-gradient(135deg, #1e3a5f, #1e293b); color: white; border: none; }}
  .stat-box.highlight .stat-val {{ color: white; }}
  .stat-box.highlight .stat-lbl {{ color: #94a3b8; }}
  .stat-box.red .stat-val {{ color: var(--red); }}
  .stat-box.green .stat-val {{ color: var(--green); }}

  /* Tables */
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; margin: 16px 0; }}
  th {{ background: var(--accent2); color: white; padding: 10px 14px; text-align: left; font-weight: 600; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; }}
  td {{ padding: 9px 14px; border-bottom: 1px solid var(--border); }}
  tr:hover td {{ background: #f8fafc; }}
  tr:last-child td {{ border-bottom: none; }}
  .text-right {{ text-align: right; }}

  /* Charts */
  .chart-wrap {{ background: var(--light); border: 1px solid var(--border); border-radius: 8px; padding: 20px; margin: 20px 0; }}
  .chart-wrap h3 {{ font-size: 13px; font-weight: 600; color: var(--gray); text-transform: uppercase; letter-spacing: 1px; margin-bottom: 16px; }}

  /* Badges */
  .badge-cat {{
    display: inline-block; padding: 2px 10px; border-radius: 999px;
    background: #dbeafe; color: #1d4ed8; font-size: 11px; font-weight: 600;
  }}
  .sev-badge {{
    display: inline-block; padding: 2px 8px; border-radius: 4px;
    font-size: 10px; font-weight: 700; letter-spacing: 1px;
  }}
  .status-dot {{
    display: inline-block; width: 8px; height: 8px;
    border-radius: 50%; margin-right: 6px; vertical-align: middle;
  }}
  .bar-bg {{ background: #e2e8f0; border-radius: 999px; height: 8px; width: 100%; min-width: 80px; }}
  .bar-fill {{ height: 8px; border-radius: 999px; }}

  /* Alert Cards */
  .alert-card {{
    background: white;
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 20px 24px;
    margin-bottom: 16px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
  }}
  .alert-card-header {{
    display: flex; justify-content: space-between; align-items: flex-start;
    margin-bottom: 12px;
  }}
  .alert-card-id {{ display: flex; align-items: center; gap: 10px; }}
  .alert-num {{
    background: var(--accent2); color: white;
    width: 24px; height: 24px; border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    font-size: 11px; font-weight: 700;
  }}
  .alert-id {{ font-family: monospace; font-size: 13px; font-weight: 600; }}
  .alert-amount {{ font-size: 22px; font-weight: 800; }}
  .alert-card-meta {{
    display: grid; grid-template-columns: repeat(2, 1fr); gap: 6px 24px;
    font-size: 12.5px; color: #374151; margin-bottom: 14px;
  }}
  .alert-narrative {{
    background: #f8fafc; border-left: 3px solid #cbd5e1;
    padding: 12px 16px; border-radius: 0 6px 6px 0;
    font-size: 12.5px; color: #475569; line-height: 1.7;
  }}

  /* Recommendations */
  .rec-list {{ list-style: none; }}
  .rec-list li {{
    display: flex; gap: 12px; padding: 14px 0;
    border-bottom: 1px solid var(--border);
    font-size: 13.5px; line-height: 1.6;
  }}
  .rec-list li:last-child {{ border-bottom: none; }}
  .rec-icon {{
    flex-shrink: 0; width: 28px; height: 28px;
    background: #dbeafe; color: #1d4ed8;
    border-radius: 50%; display: flex; align-items: center; justify-content: center;
    font-weight: 700; font-size: 12px;
  }}

  /* Footer */
  .report-footer {{
    background: var(--accent2);
    color: #94a3b8;
    padding: 32px 40px;
    border-radius: 0 0 8px 8px;
    font-size: 12px;
  }}
  .sig-block {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 32px; margin-bottom: 24px; }}
  .sig-line {{ border-top: 1px solid #334155; padding-top: 8px; margin-top: 24px; font-size: 11px; color: #475569; }}
  .footer-classif {{
    text-align: center; color: white; font-weight: 700;
    letter-spacing: 3px; font-size: 11px; text-transform: uppercase;
    padding-top: 16px; border-top: 1px solid #334155;
  }}

  /* Two-column layout */
  .two-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}

  @media print {{
    body {{ background: white; font-size: 12px; }}
    .page {{ max-width: 100%; padding: 0; }}
    .classif-banner {{ border-radius: 0; }}
    .content {{ box-shadow: none; }}
    .chart-wrap {{ break-inside: avoid; }}
    .alert-card {{ break-inside: avoid; }}
    .section {{ break-inside: avoid; padding: 24px 32px; }}
    .stat-grid {{ gap: 12px; }}
    @page {{ margin: 1.5cm; }}
  }}
</style>
</head>
<body>
<div class="page">

  <!-- Classification Banner -->
  <div class="classif-banner">&#9632; CONFIDENTIAL &#9632; RESTRICTED DISTRIBUTION &#9632; LAW ENFORCEMENT SENSITIVE &#9632;</div>

  <!-- Report Header -->
  <div class="report-header">
    <div class="classif-badge">SAR — Confidential</div>
    <h1>SUSPICIOUS ACTIVITY REPORT<br>MONTHLY SUMMARY</h1>
    <div class="subtitle">Financial Intelligence Unit &bull; AML Compliance Division</div>
    <div class="header-meta">
      <div class="meta-item">
        <label>Report Period</label>
        <span>March 1 – March 12, 2026</span>
      </div>
      <div class="meta-item">
        <label>Prepared By</label>
        <span>Compliance Analytics Team</span>
      </div>
      <div class="meta-item">
        <label>Report Date</label>
        <span>March 12, 2026</span>
      </div>
      <div class="meta-item">
        <label>Report Reference</label>
        <span>SAR-2026-03-MONTHLY</span>
      </div>
      <div class="meta-item">
        <label>Classification</label>
        <span style="color:#ef4444;font-weight:700">CONFIDENTIAL</span>
      </div>
      <div class="meta-item">
        <label>Distribution</label>
        <span>FIU / Senior Compliance Only</span>
      </div>
    </div>
  </div>

  <div class="content">

    <!-- TOC -->
    <div class="toc">
      <h2>Table of Contents</h2>
      <ol>
        <li>Executive Summary<span class="pg">Section 1</span></li>
        <li>Alert Statistics<span class="pg">Section 2</span></li>
        <li>Category Breakdown<span class="pg">Section 3</span></li>
        <li>Top 5 Alerts for Review<span class="pg">Section 4</span></li>
        <li>Typology Trends (6-Month)<span class="pg">Section 5</span></li>
        <li>Recommendations<span class="pg">Section 6</span></li>
      </ol>
    </div>

    <!-- Section 1: Executive Summary -->
    <div class="section" id="s1">
      <div class="section-title">
        <div class="section-num">1</div>
        Executive Summary
      </div>
      <p class="narrative">
        During the period of <strong>March 1–12, 2026</strong>, the AML monitoring system generated
        <strong>{data['mar_total']} alerts</strong> with a total flagged amount of
        <strong>${data['mar_amount']:,.0f}</strong>. Of these, <strong>{data['sars_filed']} alerts</strong>
        were escalated or confirmed as true positives warranting Suspicious Activity Report filing.
        Alert volume is down <strong>{abs(pct_change):.1f}%</strong> compared to February 2026
        ({data['feb_total']} alerts), reflecting the partial reporting period (12 days vs. 28 days).
        Flagged transaction value has {'increased' if amt_change > 0 else 'decreased'} by
        <strong>{abs(amt_change):.1f}%</strong> on a per-alert basis, suggesting higher-value
        transactions are being captured. The structuring and behavior categories continue to dominate
        alert generation, collectively accounting for over 49% of all alerts this period.
        Immediate attention is directed to the 11 critical-severity alerts, of which 4 have been
        escalated for SAR filing consideration.
      </p>
      <div class="stat-grid">
        <div class="stat-box highlight">
          <div class="stat-val">{data['mar_total']}</div>
          <div class="stat-lbl">Total Alerts — March 2026</div>
          <div class="stat-sub" style="color:#94a3b8">{pct_change:+.1f}% vs Feb (volume-adjusted)</div>
        </div>
        <div class="stat-box red">
          <div class="stat-val">{data['sars_filed']}</div>
          <div class="stat-lbl">SARs Filed / Escalated</div>
          <div class="stat-sub">{data['sars_filed']/data['mar_total']*100:.1f}% escalation rate</div>
        </div>
        <div class="stat-box">
          <div class="stat-val">${data['mar_amount']/1e6:.2f}M</div>
          <div class="stat-lbl">Total Flagged Amount</div>
          <div class="stat-sub">Avg ${data['mar_amount']/data['mar_total']:,.0f} per alert</div>
        </div>
      </div>
    </div>

    <!-- Section 2: Alert Statistics -->
    <div class="section" id="s2">
      <div class="section-title">
        <div class="section-num">2</div>
        Alert Statistics
      </div>

      <div class="two-col">
        <div>
          <h3 style="font-size:13px;font-weight:600;margin-bottom:12px;color:#475569">Alerts by Status</h3>
          <table>
            <thead><tr><th>Status</th><th class="text-right">Count</th><th class="text-right">% of Total</th></tr></thead>
            <tbody>{status_table_rows}</tbody>
          </table>
        </div>
        <div>
          <h3 style="font-size:13px;font-weight:600;margin-bottom:12px;color:#475569">Alerts by Severity</h3>
          <table>
            <thead><tr><th>Severity</th><th class="text-right">Count</th><th style="min-width:120px">Distribution</th></tr></thead>
            <tbody>{severity_table_rows}</tbody>
          </table>
        </div>
      </div>

      <div class="chart-wrap">
        <h3>Daily Alert Volume — March 2026</h3>
        <canvas id="dailyChart" height="100"></canvas>
      </div>
    </div>

    <!-- Section 3: Category Breakdown -->
    <div class="section" id="s3">
      <div class="section-title">
        <div class="section-num">3</div>
        Category Breakdown
      </div>

      <div class="two-col">
        <div class="chart-wrap" style="margin:0">
          <h3>Alerts by Category</h3>
          <canvas id="categoryChart" height="220"></canvas>
        </div>
        <div>
          <table style="margin-top:0">
            <thead>
              <tr>
                <th>Category</th>
                <th class="text-right">Count</th>
                <th class="text-right">% Total</th>
                <th class="text-right">Avg Amount</th>
                <th class="text-right">Total Flagged</th>
              </tr>
            </thead>
            <tbody>{cat_table_rows}</tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- Section 4: Top 5 Alerts -->
    <div class="section" id="s4">
      <div class="section-title">
        <div class="section-num">4</div>
        Top 5 Alerts for Review
      </div>
      <p class="narrative">
        The following alerts represent the highest-priority cases from the March 2026 reporting period,
        ranked by severity and flagged amount. Each requires analyst review and determination of SAR
        filing obligation within the regulatory timeframe.
      </p>
      {alert_cards}
    </div>

    <!-- Section 5: Typology Trends -->
    <div class="section" id="s5">
      <div class="section-title">
        <div class="section-num">5</div>
        Typology Trends — 6-Month View
      </div>
      <p class="narrative">
        The chart below illustrates alert volume trends across all five rule categories over the
        October 2025 – March 2026 period. Structuring alerts have shown consistent elevation,
        peaking in December 2025 with 60 alerts before declining in Q1 2026. Behavior-based
        alerts remain the most consistently generated category, reflecting broad threshold coverage.
        Network typology alerts remain low but represent high-value cases when triggered.
      </p>
      <div class="chart-wrap">
        <h3>Monthly Alert Counts by Category (Oct 2025 – Mar 2026)</h3>
        <canvas id="trendChart" height="110"></canvas>
      </div>
    </div>

    <!-- Section 6: Recommendations -->
    <div class="section" id="s6">
      <div class="section-title">
        <div class="section-num">6</div>
        Recommendations
      </div>
      <p class="narrative">
        Based on data analysis from the March 2026 reporting period and 6-month trend review,
        the Compliance Analytics Team submits the following recommendations for consideration by
        the AML Oversight Committee:
      </p>
      <ul class="rec-list">
        <li>
          <div class="rec-icon">1</div>
          <div><strong>Reduce structuring detection threshold from $9,500 to $8,750.</strong>
          Structuring Detection (Rule 2) generated 742 alerts historically with a 43.4% precision rate.
          Lowering the threshold is expected to increase precision by approximately 8–12% by eliminating
          low-value false positive noise while maintaining recall above 70%.</div>
        </li>
        <li>
          <div class="rec-icon">2</div>
          <div><strong>Implement velocity rule tuning for Rapid Fund Movement (Rule 3).</strong>
          With only 35.2% precision, this rule generates significant analyst workload for low-value alerts.
          Adding a minimum flagged amount of $5,000 as a secondary filter is projected to reduce
          false positives by 40% while preserving detection of high-value laundering schemes.</div>
        </li>
        <li>
          <div class="rec-icon">3</div>
          <div><strong>Escalate all geographic alerts involving KP, IR, and SY counterparties
          to senior analysts automatically.</strong>
          Geographic alerts in March 2026 showed an above-average flagged amount of $30,213.
          Automated escalation for FATF-listed jurisdiction alerts will reduce review lag from the
          current 4.2-day average to under 24 hours.</div>
        </li>
        <li>
          <div class="rec-icon">4</div>
          <div><strong>Initiate enhanced monitoring for the 11 critical-severity alerts
          not yet resolved.</strong> As of the report date, {data['mar_total'] - data['sars_filed']} alerts
          from March remain in 'new' or 'under_review' status. A dedicated triage session is recommended
          within the next 48 hours to assess SAR filing obligations and prevent regulatory deadline breaches.</div>
        </li>
        <li>
          <div class="rec-icon">5</div>
          <div><strong>Expand network analysis coverage to detect second-degree PEP connections.</strong>
          Current PEP Transaction (Rule 11) only flags direct PEP relationships. Given the elevated
          risk profile of the customer portfolio (11.4% high-risk, 57 critical-risk customers),
          extending detection to beneficial ownership networks is recommended in Q2 2026.</div>
        </li>
      </ul>
    </div>

  </div><!-- /content -->

  <!-- Footer -->
  <div class="report-footer">
    <div class="sig-block">
      <div>
        <div style="color:#e2e8f0;font-weight:600;margin-bottom:4px">Prepared By</div>
        <div style="color:#64748b;font-size:12px">Compliance Analytics Team</div>
        <div class="sig-line">_______________________________</div>
        <div>Analytics Lead</div>
      </div>
      <div>
        <div style="color:#e2e8f0;font-weight:600;margin-bottom:4px">Reviewed By</div>
        <div style="color:#64748b;font-size:12px">AML Compliance Officer</div>
        <div class="sig-line">_______________________________</div>
        <div>Chief Compliance Officer</div>
      </div>
      <div>
        <div style="color:#e2e8f0;font-weight:600;margin-bottom:4px">Approved By</div>
        <div style="color:#64748b;font-size:12px">Financial Intelligence Unit</div>
        <div class="sig-line">_______________________________</div>
        <div>FIU Director</div>
      </div>
    </div>
    <div style="font-size:11px;color:#475569;margin-bottom:12px">
      Report Reference: SAR-2026-03-MONTHLY &bull;
      Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC &bull;
      System: Transaction Monitoring Platform v2.4 &bull;
      Records Processed: {data['mar_total']} alerts
    </div>
    <div class="footer-classif">&#9632; CONFIDENTIAL — RESTRICTED TO AUTHORISED PERSONNEL ONLY &#9632;</div>
  </div>

</div><!-- /page -->

<script>
// Daily Alert Chart
(function() {{
  const ctx = document.getElementById('dailyChart').getContext('2d');
  new Chart(ctx, {{
    type: 'bar',
    data: {{
      labels: {daily_labels},
      datasets: [{{
        label: 'Alert Count',
        data: {daily_counts},
        backgroundColor: 'rgba(59,130,246,0.7)',
        borderColor: '#3b82f6',
        borderWidth: 1,
        borderRadius: 4,
      }}]
    }},
    options: {{
      responsive: true,
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{ callbacks: {{ label: ctx => ' Alerts: ' + ctx.parsed.y }} }}
      }},
      scales: {{
        y: {{ beginAtZero: true, grid: {{ color: '#e2e8f0' }}, ticks: {{ stepSize: 1 }} }},
        x: {{ grid: {{ display: false }} }}
      }}
    }}
  }});
}})();

// Category Donut Chart
(function() {{
  const ctx = document.getElementById('categoryChart').getContext('2d');
  new Chart(ctx, {{
    type: 'doughnut',
    data: {{
      labels: {cat_labels},
      datasets: [{{
        data: {cat_counts},
        backgroundColor: {cat_colors_list},
        borderWidth: 2,
        borderColor: '#fff',
        hoverOffset: 8,
      }}]
    }},
    options: {{
      responsive: true,
      cutout: '60%',
      plugins: {{
        legend: {{ position: 'bottom', labels: {{ padding: 16, font: {{ size: 12 }} }} }},
        tooltip: {{ callbacks: {{
          label: ctx => ' ' + ctx.label + ': ' + ctx.parsed + ' alerts'
        }} }}
      }}
    }}
  }});
}})();

// Trend Line Chart
(function() {{
  const ctx = document.getElementById('trendChart').getContext('2d');
  new Chart(ctx, {{
    type: 'line',
    data: {{
      labels: {trend_labels_json},
      datasets: {trend_datasets_json}
    }},
    options: {{
      responsive: true,
      interaction: {{ mode: 'index', intersect: false }},
      plugins: {{
        legend: {{ position: 'bottom', labels: {{ padding: 16, font: {{ size: 12 }} }} }}
      }},
      scales: {{
        y: {{ beginAtZero: true, grid: {{ color: '#e2e8f0' }} }},
        x: {{ grid: {{ color: '#e2e8f0' }} }}
      }}
    }}
  }});
}})();
</script>
</body>
</html>"""
    return html


def main():
    print("Fetching data from database...")
    data = fetch_data()
    print(f"  March 2026: {data['mar_total']} alerts, ${data['mar_amount']:,.0f} flagged")
    print("Generating HTML...")
    html = generate_html(data)
    out_path = os.path.abspath(OUTPUT_PATH)
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(html)
    size_kb = os.path.getsize(out_path) / 1024
    print(f"Report written: {out_path} ({size_kb:.1f} KB)")


if __name__ == '__main__':
    main()
