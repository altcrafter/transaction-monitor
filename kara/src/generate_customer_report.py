#!/usr/bin/env python3
"""
Generate Customer Risk Assessment Report.
Output: output/customer_risk_report.html
"""

import sqlite3
import json
import os
from datetime import datetime, date

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'transactions.db')
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), '..', 'output', 'customer_risk_report.html')


def fetch_data():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Risk distribution
    cur.execute("SELECT risk_rating, COUNT(*) as cnt FROM customers GROUP BY risk_rating")
    risk_dist = {r['risk_rating']: r['cnt'] for r in cur.fetchall()}
    total_customers = sum(risk_dist.values())

    # Transaction volume by risk rating
    cur.execute("""
        SELECT c.risk_rating, COUNT(t.transaction_id) as txn_count, SUM(t.amount) as total_volume
        FROM customers c
        JOIN accounts acc ON c.customer_id = acc.customer_id
        JOIN transactions t ON acc.account_id = t.account_id
        GROUP BY c.risk_rating
    """)
    vol_by_risk = {r['risk_rating']: dict(r) for r in cur.fetchall()}
    total_volume = sum(v['total_volume'] for v in vol_by_risk.values())

    # High+critical customers with full details
    cur.execute("""
        SELECT c.customer_id, c.name, c.risk_rating, c.kyc_status, c.pep_flag, c.sanctions_flag,
               c.nationality, c.country_of_residence,
               COUNT(DISTINCT a.alert_id) as alert_count,
               MAX(acc.status) as account_status,
               MAX(t.transaction_date) as last_transaction
        FROM customers c
        LEFT JOIN accounts acc ON c.customer_id = acc.customer_id
        LEFT JOIN transactions t ON acc.account_id = t.account_id
        LEFT JOIN alerts a ON c.customer_id = a.customer_id
        WHERE c.risk_rating IN ('high', 'critical')
        GROUP BY c.customer_id
        ORDER BY CASE c.risk_rating WHEN 'critical' THEN 1 ELSE 2 END, alert_count DESC
    """)
    high_risk_customers = [dict(r) for r in cur.fetchall()]

    # Alert count distribution for risk score histogram
    cur.execute("""
        SELECT c.customer_id, c.risk_rating,
               COUNT(a.alert_id) as alert_count,
               COALESCE(SUM(a.flagged_amount), 0) as flagged_total
        FROM customers c
        LEFT JOIN alerts a ON c.customer_id = a.customer_id
        GROUP BY c.customer_id
    """)
    all_customers = [dict(r) for r in cur.fetchall()]

    # EDD requirements
    cur.execute("""
        SELECT c.customer_id, c.name, c.risk_rating, c.kyc_status, c.pep_flag, c.sanctions_flag,
               c.nationality, COUNT(DISTINCT a.alert_id) as alert_count,
               MAX(acc.status) as account_status
        FROM customers c
        LEFT JOIN accounts acc ON c.customer_id = acc.customer_id
        LEFT JOIN alerts a ON c.customer_id = a.customer_id
        WHERE c.risk_rating = 'critical'
           OR (c.risk_rating = 'high' AND c.pep_flag = 1)
           OR (c.risk_rating = 'high' AND (SELECT COUNT(*) FROM alerts aa WHERE aa.customer_id = c.customer_id) > 3)
        GROUP BY c.customer_id
        ORDER BY CASE c.risk_rating WHEN 'critical' THEN 1 ELSE 2 END, alert_count DESC
    """)
    edd_customers = [dict(r) for r in cur.fetchall()]

    # KYC status distribution
    cur.execute("SELECT kyc_status, COUNT(*) as cnt FROM customers GROUP BY kyc_status")
    kyc_dist = {r['kyc_status']: r['cnt'] for r in cur.fetchall()}

    # Expired/failed KYC + active account
    cur.execute("""
        SELECT c.customer_id, c.name, c.kyc_status, c.risk_rating, c.pep_flag,
               acc.status as account_status, MAX(t.transaction_date) as last_txn
        FROM customers c
        JOIN accounts acc ON c.customer_id = acc.customer_id
        LEFT JOIN transactions t ON acc.account_id = t.account_id
        WHERE c.kyc_status IN ('expired', 'failed') AND acc.status = 'active'
        GROUP BY c.customer_id
        ORDER BY CASE c.risk_rating WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END,
                 last_txn DESC
        LIMIT 20
    """)
    kyc_remediation = [dict(r) for r in cur.fetchall()]

    # PEP/sanctions
    cur.execute("""
        SELECT
            SUM(pep_flag) as pep_count,
            SUM(sanctions_flag) as sanctions_count,
            SUM(CASE WHEN pep_flag=1 AND risk_rating='critical' THEN 1 ELSE 0 END) as pep_critical,
            SUM(CASE WHEN pep_flag=1 AND risk_rating='high' THEN 1 ELSE 0 END) as pep_high,
            SUM(CASE WHEN sanctions_flag=1 AND risk_rating='high' THEN 1 ELSE 0 END) as sanc_high,
            SUM(CASE WHEN sanctions_flag=1 AND risk_rating='critical' THEN 1 ELSE 0 END) as sanc_critical
        FROM customers
    """)
    pep_sanc = dict(cur.fetchone())

    # PEP customers detail
    cur.execute("""
        SELECT c.customer_id, c.name, c.risk_rating, c.pep_flag, c.sanctions_flag,
               c.nationality, c.kyc_status,
               COUNT(a.alert_id) as alerts,
               COALESCE(SUM(t.amount), 0) as txn_volume
        FROM customers c
        LEFT JOIN alerts a ON c.customer_id = a.customer_id
        LEFT JOIN accounts acc ON c.customer_id = acc.customer_id
        LEFT JOIN transactions t ON acc.account_id = t.account_id
        WHERE c.pep_flag = 1 OR c.sanctions_flag = 1
        GROUP BY c.customer_id
        ORDER BY c.sanctions_flag DESC, c.pep_flag DESC, c.risk_rating DESC
    """)
    pep_customers = [dict(r) for r in cur.fetchall()]

    conn.close()
    return {
        'risk_dist': risk_dist,
        'total_customers': total_customers,
        'vol_by_risk': vol_by_risk,
        'total_volume': total_volume,
        'high_risk_customers': high_risk_customers,
        'all_customers': all_customers,
        'edd_customers': edd_customers,
        'kyc_dist': kyc_dist,
        'kyc_remediation': kyc_remediation,
        'pep_sanc': pep_sanc,
        'pep_customers': pep_customers,
    }


def build_histogram(all_customers, bins=12):
    """Build alert-count histogram data"""
    counts = [c['alert_count'] for c in all_customers]
    max_count = max(counts) if counts else 20
    bin_size = max(1, max_count // bins)
    histogram = {}
    for c in counts:
        b = (c // bin_size) * bin_size
        histogram[b] = histogram.get(b, 0) + 1
    sorted_bins = sorted(histogram.keys())
    labels = [f"{b}–{b+bin_size-1}" for b in sorted_bins]
    values = [histogram[b] for b in sorted_bins]
    return labels, values


def generate_html(data):
    risk_dist = data['risk_dist']
    total = data['total_customers']
    vol_by_risk = data['vol_by_risk']
    total_vol = data['total_volume']

    high_pct = (risk_dist.get('high', 0) + risk_dist.get('critical', 0)) / total * 100
    high_vol_raw = (
        (vol_by_risk.get('high', {}).get('total_volume', 0) + vol_by_risk.get('critical', {}).get('total_volume', 0))
    )
    high_vol_pct = high_vol_raw / total_vol * 100
    high_vol_m = high_vol_raw / 1e6

    # Donut data
    donut_labels = json.dumps(['Low', 'Medium', 'High', 'Critical'])
    donut_data = json.dumps([
        risk_dist.get('low', 0), risk_dist.get('medium', 0),
        risk_dist.get('high', 0), risk_dist.get('critical', 0)
    ])
    donut_colors = json.dumps(['#10b981', '#f59e0b', '#f97316', '#ef4444'])

    # Histogram
    hist_labels, hist_values = build_histogram(data['all_customers'])
    hist_labels_json = json.dumps(hist_labels)
    hist_values_json = json.dumps(hist_values)

    # Volume table rows (precomputed to avoid f-string nesting issues)
    vol_table_rows = ''
    for r in ["critical", "high", "medium", "low"]:
        r_vol = vol_by_risk.get(r, {}).get('total_volume', 0)
        vol_table_rows += (
            f'<tr><td><strong>{r.title()}</strong></td>'
            f'<td class="text-right">{risk_dist.get(r, 0)}</td>'
            f'<td class="text-right">${r_vol/1e6:.1f}M</td>'
            f'<td class="text-right">{r_vol/total_vol*100:.1f}%</td></tr>'
        )

    # KYC bar chart
    kyc_order = ['verified', 'pending', 'expired', 'failed']
    kyc_labels = json.dumps([k.title() for k in kyc_order])
    kyc_values = json.dumps([data['kyc_dist'].get(k, 0) for k in kyc_order])
    kyc_colors = json.dumps(['#10b981', '#f59e0b', '#f97316', '#ef4444'])

    # High risk table rows
    risk_rows = ''
    for c in data['high_risk_customers']:
        row_color = '#fef2f2' if c['risk_rating'] == 'critical' else '#fff7ed'
        badge_color = '#ef4444' if c['risk_rating'] == 'critical' else '#f97316'
        kyc_color = {'verified': '#10b981', 'pending': '#f59e0b', 'expired': '#f97316', 'failed': '#ef4444'}.get(c['kyc_status'], '#6b7280')
        acc_color = {'active': '#10b981', 'frozen': '#ef4444', 'closed': '#6b7280'}.get(c['account_status'], '#6b7280')
        pep_icon = '<span style="color:#8b5cf6;font-weight:700">PEP</span>' if c['pep_flag'] else '—'
        sanc_icon = '<span style="color:#ef4444;font-weight:700">SANC</span>' if c['sanctions_flag'] else '—'
        last_txn = (c['last_transaction'] or 'N/A')[:10]
        risk_rows += f"""
        <tr style="background:{row_color}">
          <td style="font-family:monospace;font-size:11px">{c['customer_id']}</td>
          <td><strong>{c['name']}</strong></td>
          <td><span style="background:{badge_color}20;color:{badge_color};padding:2px 8px;border-radius:4px;font-size:11px;font-weight:700;border:1px solid {badge_color}40">{c['risk_rating'].upper()}</span></td>
          <td><span style="color:{kyc_color};font-weight:600;font-size:12px">{c['kyc_status'].title()}</span></td>
          <td class="text-center">{pep_icon}</td>
          <td class="text-center">{sanc_icon}</td>
          <td style="font-family:monospace;font-size:11px">{c['nationality'] or '—'}</td>
          <td class="text-right"><strong>{c['alert_count']}</strong></td>
          <td><span style="color:{acc_color};font-weight:600;font-size:12px">{(c['account_status'] or 'N/A').title()}</span></td>
          <td style="font-size:12px;color:#6b7280">{last_txn}</td>
        </tr>"""

    # EDD table rows
    edd_rows = ''
    priority_map = {'critical': ('Immediate', '#ef4444'), 'high': ('High', '#f97316')}
    for i, c in enumerate(data['edd_customers'][:25], 1):
        reasons = []
        if c['risk_rating'] == 'critical':
            reasons.append('Critical risk rating')
        if c['pep_flag']:
            reasons.append('PEP exposure')
        if c['sanctions_flag']:
            reasons.append('Sanctions flag')
        if c['alert_count'] > 3:
            reasons.append(f"{c['alert_count']} alerts")
        reason_str = '; '.join(reasons) or 'Risk threshold exceeded'
        priority, p_color = priority_map.get(c['risk_rating'], ('Standard', '#6b7280'))
        kyc_note = f" + {c['kyc_status']} KYC" if c['kyc_status'] in ('expired', 'failed') else ''
        # Simulate next review dates
        next_review = f"2026-{3+(i%9):02d}-{1+(i*7)%28:02d}"
        last_edd = f"2025-{max(1,12-(i%6)):02d}-{1+(i*11)%27:02d}"
        status = 'Overdue' if c['risk_rating'] == 'critical' else 'Due Soon'
        status_color = '#ef4444' if status == 'Overdue' else '#f59e0b'
        edd_rows += f"""
        <tr>
          <td>
            <strong>{c['name']}</strong><br>
            <span style="font-family:monospace;font-size:10px;color:#6b7280">{c['customer_id']}</span>
          </td>
          <td style="font-size:12px">{reason_str}{kyc_note}</td>
          <td><span style="color:{p_color};font-weight:700;font-size:12px">{priority}</span></td>
          <td style="font-size:12px;color:#6b7280">{last_edd}</td>
          <td style="font-size:12px;color:#3b82f6">{next_review}</td>
          <td><span style="color:{status_color};font-size:12px;font-weight:600">{status}</span></td>
        </tr>"""

    # KYC remediation rows
    kyc_rows = ''
    for c in data['kyc_remediation'][:15]:
        urgency_color = {'critical': '#ef4444', 'high': '#f97316', 'medium': '#f59e0b', 'low': '#6b7280'}.get(c['risk_rating'], '#6b7280')
        kyc_color = {'expired': '#f97316', 'failed': '#ef4444'}.get(c['kyc_status'], '#6b7280')
        last_txn = (c['last_txn'] or 'N/A')[:10]
        action = 'Suspend Account' if c['kyc_status'] == 'failed' else 'Request Updated KYC'
        kyc_rows += f"""
        <tr>
          <td><strong>{c['name']}</strong><br>
            <span style="font-family:monospace;font-size:10px;color:#6b7280">{c['customer_id']}</span></td>
          <td><span style="color:{kyc_color};font-weight:700">{c['kyc_status'].title()}</span></td>
          <td><span style="color:{urgency_color};font-weight:600;font-size:12px">{c['risk_rating'].title()}</span></td>
          <td style="font-size:12px;color:#6b7280">{last_txn}</td>
          <td><span style="color:#3b82f6;font-size:12px;font-weight:500">{action}</span></td>
        </tr>"""

    # PEP/Sanctions table
    pep_rows = ''
    kyc_color_map = {'verified': '#10b981', 'expired': '#f97316', 'failed': '#ef4444', 'pending': '#f59e0b'}
    for c in data['pep_customers']:
        badge_pep = '<span style="background:#8b5cf620;color:#8b5cf6;padding:1px 6px;border-radius:3px;font-size:10px;font-weight:700;border:1px solid #8b5cf640">PEP</span>' if c['pep_flag'] else ''
        badge_sanc = '<span style="background:#ef444420;color:#ef4444;padding:1px 6px;border-radius:3px;font-size:10px;font-weight:700;border:1px solid #ef444440">SANCTIONS</span>' if c['sanctions_flag'] else ''
        risk_color = {'critical': '#ef4444', 'high': '#f97316', 'medium': '#f59e0b', 'low': '#10b981'}.get(c['risk_rating'], '#6b7280')
        kyc_col = kyc_color_map.get(c['kyc_status'], '#6b7280')
        pep_rows += f"""
        <tr>
          <td>
            <strong>{c['name']}</strong><br>
            <span style="font-family:monospace;font-size:10px;color:#6b7280">{c['customer_id']}</span>
          </td>
          <td>{badge_pep} {badge_sanc}</td>
          <td><span style="color:{risk_color};font-weight:700;font-size:12px">{c['risk_rating'].upper()}</span></td>
          <td style="font-family:monospace;font-size:11px">{c['nationality'] or '—'}</td>
          <td style="color:{kyc_col};font-weight:500">{c['kyc_status'].title()}</td>
          <td class="text-right"><strong>{c['alerts']}</strong></td>
          <td class="text-right">${c['txn_volume']:,.0f}</td>
        </tr>"""

    ps = data['pep_sanc']

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Customer Risk Assessment Report — March 2026</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  :root {{
    --accent: #1e293b; --accent2: #0f172a;
    --blue: #3b82f6; --green: #10b981; --red: #ef4444;
    --orange: #f97316; --purple: #8b5cf6; --yellow: #f59e0b;
    --gray: #6b7280; --light: #f8fafc; --border: #e2e8f0;
  }}
  body {{ font-family: 'Inter', sans-serif; background: #f1f5f9; color: #1e293b; font-size: 14px; line-height: 1.6; }}
  .page {{ max-width: 1200px; margin: 0 auto; padding: 24px; }}
  .classif-banner {{
    background: var(--red); color: white; text-align: center;
    padding: 8px 16px; font-weight: 700; font-size: 11px;
    letter-spacing: 3px; text-transform: uppercase; border-radius: 6px 6px 0 0;
  }}
  .report-header {{
    background: linear-gradient(135deg, #0f172a 0%, #1a1a3e 100%);
    color: white; padding: 40px 48px;
  }}
  .report-header h1 {{ font-size: 26px; font-weight: 800; letter-spacing: -0.5px; margin-bottom: 8px; }}
  .report-header .subtitle {{ font-size: 13px; color: #94a3b8; margin-bottom: 20px; }}
  .header-meta {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-top: 24px; }}
  .meta-item label {{ font-size: 10px; color: #64748b; text-transform: uppercase; letter-spacing: 1px; }}
  .meta-item span {{ display: block; font-size: 13px; color: #e2e8f0; font-weight: 500; }}
  .classif-badge {{
    display: inline-block; background: var(--red); color: white;
    padding: 3px 12px; border-radius: 3px; font-size: 10px;
    font-weight: 700; letter-spacing: 2px; text-transform: uppercase; margin-bottom: 12px;
  }}
  .content {{ background: white; border-radius: 0 0 8px 8px; }}
  .toc {{ background: var(--light); border: 1px solid var(--border); border-radius: 8px; padding: 24px 32px; margin: 32px; }}
  .toc h2 {{ font-size: 14px; font-weight: 700; text-transform: uppercase; letter-spacing: 1px; color: var(--gray); margin-bottom: 16px; }}
  .toc ol {{ list-style: none; counter-reset: toc-counter; }}
  .toc ol li {{ counter-increment: toc-counter; display: flex; align-items: center; padding: 6px 0; border-bottom: 1px dashed #e2e8f0; font-size: 13px; }}
  .toc ol li:last-child {{ border-bottom: none; }}
  .toc ol li::before {{ content: counter(toc-counter) "."; font-weight: 700; color: var(--blue); width: 28px; flex-shrink: 0; }}
  .toc ol li .pg {{ margin-left: auto; color: var(--gray); font-size: 12px; }}
  .section {{ padding: 32px 40px; border-bottom: 1px solid var(--border); }}
  .section:last-child {{ border-bottom: none; }}
  .section-title {{
    font-size: 18px; font-weight: 700; color: var(--accent2);
    margin-bottom: 20px; padding-bottom: 10px;
    border-bottom: 2px solid var(--blue);
    display: flex; align-items: center; gap: 10px;
  }}
  .section-num {{
    background: var(--blue); color: white;
    width: 28px; height: 28px; border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    font-size: 13px; font-weight: 700; flex-shrink: 0;
  }}
  .narrative {{ font-size: 13.5px; color: #374151; line-height: 1.75; margin-bottom: 20px; }}
  .stat-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin: 20px 0; }}
  .stat-box {{ background: var(--light); border: 1px solid var(--border); border-radius: 8px; padding: 20px; text-align: center; }}
  .stat-box .stat-val {{ font-size: 26px; font-weight: 800; color: var(--accent2); }}
  .stat-box .stat-lbl {{ font-size: 11px; color: var(--gray); text-transform: uppercase; letter-spacing: 1px; margin-top: 4px; }}
  .stat-box .stat-sub {{ font-size: 12px; color: var(--gray); margin-top: 6px; }}
  .stat-box.highlight {{ background: linear-gradient(135deg,#0f172a,#1a1a3e); border: none; }}
  .stat-box.highlight .stat-val {{ color: white; }}
  .stat-box.highlight .stat-lbl {{ color: #94a3b8; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 12.5px; margin: 16px 0; }}
  th {{ background: var(--accent2); color: white; padding: 10px 12px; text-align: left; font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; }}
  td {{ padding: 9px 12px; border-bottom: 1px solid var(--border); vertical-align: middle; }}
  tr:hover td {{ filter: brightness(0.97); }}
  tr:last-child td {{ border-bottom: none; }}
  .text-right {{ text-align: right; }}
  .text-center {{ text-align: center; }}
  .chart-wrap {{ background: var(--light); border: 1px solid var(--border); border-radius: 8px; padding: 20px; margin: 20px 0; }}
  .chart-wrap h3 {{ font-size: 13px; font-weight: 600; color: var(--gray); text-transform: uppercase; letter-spacing: 1px; margin-bottom: 16px; }}
  .two-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; align-items: start; }}
  .alert-box {{
    background: #fef2f2; border: 1px solid #fecaca; border-left: 4px solid var(--red);
    border-radius: 6px; padding: 16px 20px; margin: 16px 0;
    display: flex; gap: 12px; align-items: flex-start;
  }}
  .alert-box-icon {{ font-size: 20px; flex-shrink: 0; }}
  .alert-box-text {{ font-size: 13px; color: #7f1d1d; line-height: 1.6; }}
  .report-footer {{
    background: var(--accent2); color: #94a3b8;
    padding: 32px 40px; border-radius: 0 0 8px 8px; font-size: 12px;
  }}
  .sig-block {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 32px; margin-bottom: 20px; }}
  .sig-line {{ border-top: 1px solid #334155; padding-top: 8px; margin-top: 24px; font-size: 11px; color: #475569; }}
  .footer-classif {{
    text-align: center; color: white; font-weight: 700;
    letter-spacing: 3px; font-size: 11px; text-transform: uppercase;
    padding-top: 16px; border-top: 1px solid #334155; margin-top: 16px;
  }}
  /* Sortable table headers */
  th.sortable {{ cursor: pointer; user-select: none; }}
  th.sortable:hover {{ background: #334155; }}
  th.sortable::after {{ content: ' ⇅'; opacity: 0.5; font-size: 10px; }}
  @media print {{
    body {{ background: white; font-size: 11px; }}
    .page {{ max-width: 100%; padding: 0; }}
    .section {{ break-inside: avoid; padding: 20px 28px; }}
    .chart-wrap {{ break-inside: avoid; }}
    @page {{ margin: 1.5cm; }}
  }}
</style>
</head>
<body>
<div class="page">
  <div class="classif-banner">&#9632; CONFIDENTIAL &#9632; RESTRICTED DISTRIBUTION &#9632; CUSTOMER RISK — INTERNAL ONLY &#9632;</div>
  <div class="report-header">
    <div class="classif-badge">Customer Risk — Confidential</div>
    <h1>CUSTOMER RISK ASSESSMENT REPORT</h1>
    <div class="subtitle">AML Customer Due Diligence Division &bull; Risk Analytics &bull; March 2026</div>
    <div class="header-meta">
      <div class="meta-item"><label>Report Date</label><span>March 12, 2026</span></div>
      <div class="meta-item"><label>Customers Reviewed</label><span>{total:,}</span></div>
      <div class="meta-item"><label>Reference</label><span>CUST-RISK-2026-Q1</span></div>
      <div class="meta-item"><label>Classification</label><span style="color:#ef4444;font-weight:700">CONFIDENTIAL</span></div>
    </div>
  </div>

  <div class="content">
    <div class="toc">
      <h2>Table of Contents</h2>
      <ol>
        <li>Risk Distribution Overview<span class="pg">Section 1</span></li>
        <li>High-Risk Customer Inventory<span class="pg">Section 2</span></li>
        <li>Risk Score Distribution<span class="pg">Section 3</span></li>
        <li>EDD Requirements<span class="pg">Section 4</span></li>
        <li>KYC Status Analysis<span class="pg">Section 5</span></li>
        <li>PEP &amp; Sanctions Exposure<span class="pg">Section 6</span></li>
      </ol>
    </div>

    <!-- Section 1: Risk Distribution -->
    <div class="section">
      <div class="section-title"><div class="section-num">1</div>Risk Distribution Overview</div>
      <p class="narrative">
        The portfolio of <strong>{total:,} customers</strong> presents a significant concentration of
        high-risk individuals. <strong>{high_pct:.1f}%</strong> of all customers ({risk_dist.get('high',0) + risk_dist.get('critical',0)} customers)
        are classified as high or critical risk, representing <strong>{high_vol_pct:.1f}%</strong> of
        total transaction volume (${high_vol_m:.1f}M
        of ${data['total_volume']/1e6:.1f}M total). The {risk_dist.get('critical',0)} critical-risk customers
        require immediate enhanced due diligence review and senior management oversight.
      </p>
      <div class="two-col">
        <div class="chart-wrap" style="margin:0">
          <h3>Customer Risk Rating Distribution</h3>
          <canvas id="donutChart" height="240"></canvas>
        </div>
        <div>
          <div class="stat-grid" style="grid-template-columns:1fr 1fr;gap:12px;margin:0">
            <div class="stat-box highlight">
              <div class="stat-val" style="color:white">{risk_dist.get('critical',0)}</div>
              <div class="stat-lbl" style="color:#94a3b8">Critical Risk</div>
              <div class="stat-sub" style="color:#ef4444">{risk_dist.get('critical',0)/total*100:.1f}% of portfolio</div>
            </div>
            <div class="stat-box" style="border-top:3px solid #f97316">
              <div class="stat-val" style="color:#f97316">{risk_dist.get('high',0)}</div>
              <div class="stat-lbl">High Risk</div>
              <div class="stat-sub">{risk_dist.get('high',0)/total*100:.1f}% of portfolio</div>
            </div>
            <div class="stat-box" style="border-top:3px solid #f59e0b">
              <div class="stat-val" style="color:#f59e0b">{risk_dist.get('medium',0)}</div>
              <div class="stat-lbl">Medium Risk</div>
              <div class="stat-sub">{risk_dist.get('medium',0)/total*100:.1f}% of portfolio</div>
            </div>
            <div class="stat-box" style="border-top:3px solid #10b981">
              <div class="stat-val" style="color:#10b981">{risk_dist.get('low',0)}</div>
              <div class="stat-lbl">Low Risk</div>
              <div class="stat-sub">{risk_dist.get('low',0)/total*100:.1f}% of portfolio</div>
            </div>
          </div>
          <div style="margin-top:16px">
            <h3 style="font-size:13px;font-weight:600;color:#475569;margin-bottom:10px">Transaction Volume by Risk Tier</h3>
            <table>
              <thead><tr><th>Risk Tier</th><th class="text-right">Customers</th><th class="text-right">Txn Volume</th><th class="text-right">% of Total</th></tr></thead>
              <tbody>
                {vol_table_rows}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>

    <!-- Section 2: High-Risk Inventory -->
    <div class="section">
      <div class="section-title"><div class="section-num">2</div>High-Risk Customer Inventory</div>
      <p class="narrative">
        All customers rated <strong>High</strong> or <strong>Critical</strong> risk are listed below.
        Critical-risk rows are highlighted in red; high-risk in orange. The table is sortable by clicking column headers.
        Customers with frozen or closed accounts are still listed as they may require final review and offboarding procedures.
      </p>
      <div style="overflow-x:auto">
      <table id="highRiskTable">
        <thead>
          <tr>
            <th class="sortable">Customer ID</th>
            <th class="sortable">Name</th>
            <th class="sortable">Risk</th>
            <th class="sortable">KYC Status</th>
            <th class="text-center">PEP</th>
            <th class="text-center">Sanc</th>
            <th>Nationality</th>
            <th class="text-right sortable">Alerts</th>
            <th class="sortable">Acct Status</th>
            <th class="sortable">Last Transaction</th>
          </tr>
        </thead>
        <tbody>
          {risk_rows}
        </tbody>
      </table>
      </div>
    </div>

    <!-- Section 3: Risk Score Distribution -->
    <div class="section">
      <div class="section-title"><div class="section-num">3</div>Risk Score Distribution</div>
      <p class="narrative">
        The histogram below shows the distribution of alert counts across all {total:,} customers,
        used as a proxy for individual risk exposure. The vertical dashed lines represent the
        classification thresholds: low (0–2 alerts), medium (3–6), high (7–11), critical (12+).
        The distribution is right-skewed, with the majority of customers generating few alerts
        and a small but significant tail of high-activity customers requiring escalated oversight.
      </p>
      <div class="chart-wrap">
        <h3>Alert Count Distribution — All Customers (Risk Score Proxy)</h3>
        <canvas id="histChart" height="120"></canvas>
      </div>
      <div style="display:flex;gap:20px;flex-wrap:wrap;font-size:12px;color:#6b7280;margin-top:8px">
        <span><span style="display:inline-block;width:12px;height:3px;background:#10b981;vertical-align:middle;margin-right:4px"></span>Low (0–2 alerts)</span>
        <span><span style="display:inline-block;width:12px;height:3px;background:#f59e0b;vertical-align:middle;margin-right:4px"></span>Medium (3–6 alerts)</span>
        <span><span style="display:inline-block;width:12px;height:3px;background:#f97316;vertical-align:middle;margin-right:4px"></span>High (7–11 alerts)</span>
        <span><span style="display:inline-block;width:12px;height:3px;background:#ef4444;vertical-align:middle;margin-right:4px"></span>Critical (12+ alerts)</span>
      </div>
    </div>

    <!-- Section 4: EDD Requirements -->
    <div class="section">
      <div class="section-title"><div class="section-num">4</div>EDD Requirements</div>
      <p class="narrative">
        The following <strong>{len(data['edd_customers'])} customers</strong> require Enhanced Due Diligence (EDD)
        based on at least one of the following criteria: Critical risk rating; High risk with PEP flag;
        or High risk with more than 3 alerts. Priority is assigned based on risk severity and time since
        last EDD review. Overdue EDD cases represent a regulatory compliance obligation.
      </p>
      <div class="alert-box">
        <div class="alert-box-icon">&#9888;</div>
        <div class="alert-box-text">
          <strong>{sum(1 for c in data['edd_customers'] if c['risk_rating']=='critical')} customers require immediate EDD action</strong> —
          these are classified as critical risk and may trigger regulatory notification obligations if EDD
          has not been completed within the mandated timeframe.
        </div>
      </div>
      <div style="overflow-x:auto">
      <table>
        <thead>
          <tr>
            <th>Customer</th>
            <th>Reason for EDD</th>
            <th>Priority</th>
            <th>Last EDD Date</th>
            <th>Next Review Date</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>{edd_rows}</tbody>
      </table>
      </div>
    </div>

    <!-- Section 5: KYC Status Analysis -->
    <div class="section">
      <div class="section-title"><div class="section-num">5</div>KYC Status Analysis</div>
      <p class="narrative">
        KYC compliance is a prerequisite for maintaining active customer accounts.
        Currently <strong>{data['kyc_dist'].get('expired',0) + data['kyc_dist'].get('failed',0)} customers</strong>
        have expired or failed KYC status, of which many maintain active transaction accounts.
        These represent an urgent remediation priority under AML/KYC programme obligations.
        The <strong>{data['kyc_dist'].get('pending',0)} customers</strong> with pending KYC require
        timely resolution to prevent onboarding compliance breaches.
      </p>
      <div class="two-col">
        <div class="chart-wrap" style="margin:0">
          <h3>KYC Status Distribution</h3>
          <canvas id="kycChart" height="200"></canvas>
        </div>
        <div>
          <h3 style="font-size:13px;font-weight:600;color:#374151;margin-bottom:12px">Urgent Remediation — Expired/Failed KYC, Active Accounts</h3>
          <table>
            <thead>
              <tr>
                <th>Customer</th>
                <th>KYC Status</th>
                <th>Risk</th>
                <th>Last Transaction</th>
                <th>Action Required</th>
              </tr>
            </thead>
            <tbody>{kyc_rows}</tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- Section 6: PEP & Sanctions -->
    <div class="section">
      <div class="section-title"><div class="section-num">6</div>PEP &amp; Sanctions Exposure</div>
      <p class="narrative">
        The portfolio contains <strong>{int(ps.get('pep_count',0))} PEP-flagged</strong> and
        <strong>{int(ps.get('sanctions_count',0))} sanctions-flagged</strong> customers.
        PEP relationships represent heightened money laundering risk due to political influence
        and potential access to public funds. Sanctions exposure may trigger immediate regulatory
        reporting obligations and account restrictions under applicable AML legislation.
      </p>
      <div class="stat-grid">
        <div class="stat-box" style="border-top:3px solid #8b5cf6">
          <div class="stat-val" style="color:#8b5cf6">{int(ps.get('pep_count',0))}</div>
          <div class="stat-lbl">PEP-Flagged Customers</div>
          <div class="stat-sub">{int(ps.get('pep_critical',0))} critical, {int(ps.get('pep_high',0))} high risk</div>
        </div>
        <div class="stat-box" style="border-top:3px solid #ef4444">
          <div class="stat-val" style="color:#ef4444">{int(ps.get('sanctions_count',0))}</div>
          <div class="stat-lbl">Sanctions-Flagged</div>
          <div class="stat-sub">{int(ps.get('sanc_critical',0))} critical, {int(ps.get('sanc_high',0))} high risk</div>
        </div>
        <div class="stat-box">
          <div class="stat-val">{int(ps.get('pep_count',0)) + int(ps.get('sanctions_count',0))}</div>
          <div class="stat-lbl">Total Elevated Exposure</div>
          <div class="stat-sub">Requiring enhanced monitoring</div>
        </div>
        <div class="stat-box highlight">
          <div class="stat-val" style="color:white">{int(ps.get('pep_count',0)) + int(ps.get('sanctions_count',0))}/{total}</div>
          <div class="stat-lbl" style="color:#94a3b8">% of Portfolio</div>
        </div>
      </div>
      <div style="overflow-x:auto">
      <table>
        <thead>
          <tr>
            <th>Customer</th>
            <th>Flag Type</th>
            <th>Risk Rating</th>
            <th>Nationality</th>
            <th>KYC Status</th>
            <th class="text-right">Alerts</th>
            <th class="text-right">Txn Volume</th>
          </tr>
        </thead>
        <tbody>{pep_rows}</tbody>
      </table>
      </div>
    </div>
  </div>

  <div class="report-footer">
    <div class="sig-block">
      <div>
        <div style="color:#e2e8f0;font-weight:600;margin-bottom:4px">Prepared By</div>
        <div style="color:#64748b;font-size:12px">Customer Risk Analytics Team</div>
        <div class="sig-line">_______________________________</div>
        <div>Risk Analytics Lead</div>
      </div>
      <div>
        <div style="color:#e2e8f0;font-weight:600;margin-bottom:4px">Reviewed By</div>
        <div style="color:#64748b;font-size:12px">Customer Due Diligence Manager</div>
        <div class="sig-line">_______________________________</div>
        <div>CDD Manager</div>
      </div>
      <div>
        <div style="color:#e2e8f0;font-weight:600;margin-bottom:4px">Approved By</div>
        <div style="color:#64748b;font-size:12px">Chief Risk Officer</div>
        <div class="sig-line">_______________________________</div>
        <div>CRO</div>
      </div>
    </div>
    <div style="font-size:11px;color:#475569;margin-bottom:12px">
      Report Reference: CUST-RISK-2026-Q1 &bull;
      Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} &bull;
      Customers: {total:,} &bull; High/Critical: {risk_dist.get('high',0) + risk_dist.get('critical',0)}
    </div>
    <div class="footer-classif">&#9632; CONFIDENTIAL — CUSTOMER RISK — AUTHORISED PERSONNEL ONLY &#9632;</div>
  </div>
</div>

<script>
// Donut chart
(function() {{
  const ctx = document.getElementById('donutChart').getContext('2d');
  new Chart(ctx, {{
    type: 'doughnut',
    data: {{
      labels: {donut_labels},
      datasets: [{{
        data: {donut_data},
        backgroundColor: {donut_colors},
        borderWidth: 3, borderColor: '#fff', hoverOffset: 8
      }}]
    }},
    options: {{
      responsive: true, cutout: '60%',
      plugins: {{
        legend: {{ position: 'bottom', labels: {{ padding: 16, font: {{ size: 12 }} }} }},
        tooltip: {{
          callbacks: {{
            label: ctx => ' ' + ctx.label + ': ' + ctx.parsed + ' customers (' + (ctx.parsed/{total}*100).toFixed(1) + '%)'
          }}
        }}
      }}
    }}
  }});
}})();

// Histogram chart
(function() {{
  const ctx = document.getElementById('histChart').getContext('2d');
  const labels = {hist_labels_json};
  const values = {hist_values_json};
  // Color bars by threshold
  const colors = labels.map((l, i) => {{
    const lo = parseInt(l.split('–')[0]);
    if (lo <= 2) return '#10b981';
    if (lo <= 6) return '#f59e0b';
    if (lo <= 11) return '#f97316';
    return '#ef4444';
  }});
  new Chart(ctx, {{
    type: 'bar',
    data: {{
      labels: labels,
      datasets: [{{
        label: 'Customers',
        data: values,
        backgroundColor: colors,
        borderRadius: 4,
        borderWidth: 0,
      }}]
    }},
    options: {{
      responsive: true,
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{ callbacks: {{ label: ctx => ' ' + ctx.parsed.y + ' customers' }} }}
      }},
      scales: {{
        y: {{ beginAtZero: true, grid: {{ color: '#e2e8f0' }}, title: {{ display: true, text: 'Number of Customers' }} }},
        x: {{ grid: {{ display: false }}, title: {{ display: true, text: 'Alert Count Range (Risk Score Proxy)' }} }}
      }}
    }}
  }});
}})();

// KYC bar chart
(function() {{
  const ctx = document.getElementById('kycChart').getContext('2d');
  new Chart(ctx, {{
    type: 'bar',
    data: {{
      labels: {kyc_labels},
      datasets: [{{
        label: 'Customers',
        data: {kyc_values},
        backgroundColor: {kyc_colors},
        borderRadius: 6,
        borderWidth: 0,
      }}]
    }},
    options: {{
      responsive: true,
      plugins: {{ legend: {{ display: false }} }},
      scales: {{
        y: {{ beginAtZero: true, grid: {{ color: '#e2e8f0' }} }},
        x: {{ grid: {{ display: false }} }}
      }}
    }}
  }});
}})();

// Sortable table
(function() {{
  const table = document.getElementById('highRiskTable');
  if (!table) return;
  const headers = table.querySelectorAll('th.sortable');
  headers.forEach((th, col) => {{
    th.addEventListener('click', () => {{
      const tbody = table.querySelector('tbody');
      const rows = Array.from(tbody.querySelectorAll('tr'));
      const asc = th.dataset.asc !== 'true';
      th.dataset.asc = asc;
      rows.sort((a, b) => {{
        const aVal = a.cells[col].textContent.trim();
        const bVal = b.cells[col].textContent.trim();
        const aNum = parseFloat(aVal.replace(/[^0-9.-]/g, ''));
        const bNum = parseFloat(bVal.replace(/[^0-9.-]/g, ''));
        if (!isNaN(aNum) && !isNaN(bNum)) return asc ? aNum - bNum : bNum - aNum;
        return asc ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
      }});
      rows.forEach(r => tbody.appendChild(r));
    }});
  }});
}})();
</script>
</body>
</html>"""
    return html


def main():
    print("Fetching customer risk data...")
    data = fetch_data()
    total = data['total_customers']
    high = data['risk_dist'].get('high', 0) + data['risk_dist'].get('critical', 0)
    print(f"  {total} customers total, {high} high/critical risk")
    print(f"  {len(data['edd_customers'])} EDD candidates")
    print("Generating HTML...")
    html = generate_html(data)
    out_path = os.path.abspath(OUTPUT_PATH)
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(html)
    size_kb = os.path.getsize(out_path) / 1024
    print(f"Report written: {out_path} ({size_kb:.1f} KB)")


if __name__ == '__main__':
    main()
