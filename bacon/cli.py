"""
cli.py — Command-line interface for the Transaction Monitoring Platform.

Commands:
  init      Initialize the database (schema + seed data)
  generate  Run the data generator
  etl       Run the ETL enrichment pipeline
  rules     Seed and run the rule engine
  api       Start the Flask API server
  test      Run the full test suite
  export    Export data to CSV files
  analyze   Run all analysis queries
  stats     Print quick platform statistics
  full-run  Run init -> generate -> etl -> rules in sequence

Usage:
  python cli.py <command> [options]
"""

from __future__ import annotations

import sys
import argparse
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

import db


def cmd_init(args) -> None:
    """Initialize or reset the database."""
    if db.DB_PATH.exists() and not args.force:
        print(f"[cli] Database already exists at {db.DB_PATH}")
        print(f"[cli] Use --force to reinitialize.")
        return
    if db.DB_PATH.exists():
        db.DB_PATH.unlink()
        print(f"[cli] Removed existing database.")
    db.init_db()
    conn = db.get_connection()
    try:
        counts = db.get_row_counts(conn)
        print(f"\n[cli] Tables created: {len(counts)}")
        for tbl, n in sorted(counts.items()):
            print(f"  {tbl:<30} {n:>8} rows")
    finally:
        conn.close()


def cmd_generate(args) -> None:
    """Run the synthetic data generator."""
    import data_generator
    data_generator.main()


def cmd_etl(args) -> None:
    """Run the ETL enrichment pipeline."""
    import etl
    etl.main()


def cmd_rules(args) -> None:
    """Seed rules and run the rule engine."""
    import rule_engine
    rule_engine.main()


def cmd_api(args) -> None:
    """Start the Flask API server."""
    import src.api as api
    print(f"[cli] Starting API on port {args.port}")
    api.app.run(debug=args.debug, port=args.port, host=args.host)


def cmd_test(args) -> None:
    """Run the full test suite."""
    import subprocess
    result = subprocess.run([sys.executable, "tests/test_suite.py"], capture_output=False)
    sys.exit(result.returncode)


def cmd_export(args) -> None:
    """Export all data to CSV files."""
    import export
    export.main()


def cmd_analyze(args) -> None:
    """Run all analysis queries."""
    import analysis_queries
    analysis_queries.main()


def cmd_stats(args) -> None:
    """Print a quick statistics summary."""
    conn = db.get_connection()
    try:
        counts = db.get_row_counts(conn)
        print("\n=== Platform Statistics ===")
        for tbl, n in sorted(counts.items()):
            if tbl not in ('sqlite_sequence',):
                print(f"  {tbl:<30} {n:>10,} rows")

        # Key metrics
        total_vol = conn.execute(
            "SELECT ROUND(SUM(amount_usd)/1e9, 2) FROM transaction_enrichment"
        ).fetchone()[0]
        open_alerts = conn.execute(
            "SELECT COUNT(*) FROM alerts WHERE status='open'"
        ).fetchone()[0]
        high_alerts = conn.execute(
            "SELECT COUNT(*) FROM alerts WHERE score >= 60"
        ).fetchone()[0]
        print(f"\n  Total volume processed:  ${total_vol}B USD")
        print(f"  Open alerts:             {open_alerts:,}")
        print(f"  High-priority alerts:    {high_alerts:,} (score >= 60)")
    finally:
        conn.close()


def cmd_full_run(args) -> None:
    """Run the complete pipeline from scratch."""
    print("=== FULL PIPELINE RUN ===")

    class FakeArgs:
        force = True

    cmd_init(FakeArgs())

    import data_generator
    data_generator.main()

    import etl
    etl.main()

    import rule_engine
    rule_engine.main()

    print("\n=== Full pipeline complete ===")
    cmd_stats(args)


def cmd_optimize(args) -> None:
    """Run EXPLAIN QUERY PLAN on key queries and suggest optimizations."""
    conn = db.get_connection()
    try:
        queries = {
            "Transaction listing": """
                SELECT t.id, t.timestamp, te.amount_usd
                FROM transactions t
                JOIN transaction_enrichment te ON te.transaction_id = t.id
                WHERE t.timestamp >= '2024-06-01' AND t.timestamp < '2024-07-01'
                ORDER BY t.timestamp DESC LIMIT 50
            """,
            "Alert with joins": """
                SELECT al.*, r.name, c.name
                FROM alerts al
                JOIN rules r ON r.id = al.rule_id
                JOIN transactions t ON t.id = al.transaction_id
                JOIN accounts a ON a.id = t.account_id
                JOIN customers c ON c.id = a.customer_id
                WHERE al.score >= 60
                ORDER BY al.score DESC LIMIT 20
            """,
            "Velocity lookup": """
                SELECT COUNT(*) FROM transaction_enrichment
                WHERE velocity_1h >= 20
            """,
        }
        print("\n=== EXPLAIN QUERY PLAN ===")
        for name, sql in queries.items():
            print(f"\n  [{name}]")
            plan = conn.execute(f"EXPLAIN QUERY PLAN {sql}").fetchall()
            for row in plan:
                print(f"    {dict(row)}")
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Argument parsing
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Transaction Monitor — Backend Engine CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", metavar="command")

    # init
    p_init = sub.add_parser("init", help="Initialize the database")
    p_init.add_argument("--force", action="store_true", help="Drop and recreate")

    # generate
    sub.add_parser("generate", help="Run data generator")

    # etl
    sub.add_parser("etl", help="Run ETL enrichment")

    # rules
    sub.add_parser("rules", help="Run rule engine")

    # api
    p_api = sub.add_parser("api", help="Start Flask API server")
    p_api.add_argument("--port", type=int, default=5000)
    p_api.add_argument("--host", default="127.0.0.1")
    p_api.add_argument("--debug", action="store_true")

    # test
    sub.add_parser("test", help="Run test suite")

    # export
    sub.add_parser("export", help="Export to CSV")

    # analyze
    sub.add_parser("analyze", help="Run analysis queries")

    # stats
    sub.add_parser("stats", help="Show platform statistics")

    # full-run
    sub.add_parser("full-run", help="Run complete pipeline from scratch")

    # optimize
    sub.add_parser("optimize", help="Run EXPLAIN on key queries")

    args = parser.parse_args()

    commands = {
        "init":     cmd_init,
        "generate": cmd_generate,
        "etl":      cmd_etl,
        "rules":    cmd_rules,
        "api":      cmd_api,
        "test":     cmd_test,
        "export":   cmd_export,
        "analyze":  cmd_analyze,
        "stats":    cmd_stats,
        "full-run": cmd_full_run,
        "optimize": cmd_optimize,
    }

    if not args.command:
        parser.print_help()
        return

    handler = commands.get(args.command)
    if handler:
        handler(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
