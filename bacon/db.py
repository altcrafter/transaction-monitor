"""
db.py — Database connection and utility helpers.

Provides a single get_connection() factory that enforces:
  - Foreign key constraints
  - WAL journal mode for concurrent reads
  - Row-factory returning sqlite3.Row objects (dict-like access)
"""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent
DB_PATH      = PROJECT_ROOT / "data" / "transactions.db"
SCHEMA_PATH  = PROJECT_ROOT / "schema.sql"


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Open (or create) the SQLite database and configure it.

    Args:
        db_path: Path to the SQLite file. Created if it does not exist.

    Returns:
        A configured sqlite3.Connection with row_factory set to sqlite3.Row.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA cache_size = -64000;")   # 64 MB cache
    conn.execute("PRAGMA temp_store = MEMORY;")
    return conn


@contextmanager
def db_conn(db_path: Path = DB_PATH) -> Generator[sqlite3.Connection, None, None]:
    """Context manager that yields a connection and commits/rolls-back on exit.

    Usage::

        with db_conn() as conn:
            conn.execute("INSERT ...")

    Yields:
        sqlite3.Connection
    """
    conn = get_connection(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: Path = DB_PATH, schema_path: Path = SCHEMA_PATH) -> None:
    """Create all tables from schema.sql if they do not exist.

    Also writes an audit_log entry on success.

    Args:
        db_path: Path to the SQLite file.
        schema_path: Path to the SQL schema file.
    """
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    sql = schema_path.read_text(encoding="utf-8")

    with db_conn(db_path) as conn:
        conn.executescript(sql)
        # Log the initialization
        conn.execute(
            """INSERT INTO audit_log (event_type, description, metadata)
               VALUES ('schema_init', 'Database initialized from schema.sql', ?)""",
            (json.dumps({"schema_path": str(schema_path), "db_path": str(db_path)}),),
        )

    print(f"[db] Database initialized at: {db_path}")


def get_table_info(conn: sqlite3.Connection) -> dict[str, list[dict[str, Any]]]:
    """Return a mapping of table_name -> list of column descriptors.

    Args:
        conn: An open sqlite3 connection.

    Returns:
        Dict mapping table names to their PRAGMA table_info rows.
    """
    tables_q = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
    )
    tables = [row["name"] for row in tables_q.fetchall()]
    result: dict[str, list[dict[str, Any]]] = {}
    for tbl in tables:
        cols = conn.execute(f"PRAGMA table_info({tbl});").fetchall()
        result[tbl] = [dict(c) for c in cols]
    return result


def get_row_counts(conn: sqlite3.Connection) -> dict[str, int]:
    """Return {table_name: row_count} for all tables.

    Args:
        conn: An open sqlite3 connection.

    Returns:
        Dict mapping table names to their row counts.
    """
    tables_q = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
    )
    tables = [row["name"] for row in tables_q.fetchall()]
    counts: dict[str, int] = {}
    for tbl in tables:
        (n,) = conn.execute(f"SELECT COUNT(*) FROM {tbl};").fetchone()
        counts[tbl] = n
    return counts


# ---------------------------------------------------------------------------
# CLI: python db.py
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys

    print("=" * 60)
    print("Transaction Monitor — Database Initialization")
    print("=" * 60)

    # Allow override via env or arg
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else DB_PATH

    init_db(path)

    conn = get_connection(path)
    try:
        info = get_table_info(conn)
        counts = get_row_counts(conn)

        print(f"\n{'Table':<25} {'Columns':>8} {'Rows':>8}")
        print("-" * 45)
        for tbl, cols in sorted(info.items()):
            print(f"  {tbl:<23} {len(cols):>8} {counts.get(tbl, 0):>8}")

        # Verify country_risk seed data
        cr = conn.execute("SELECT risk_level, COUNT(*) n FROM country_risk GROUP BY risk_level ORDER BY risk_level;").fetchall()
        print("\nCountry risk distribution:")
        labels = {1: "Low", 2: "Medium", 3: "High", 4: "Very High", 5: "Sanctioned"}
        for row in cr:
            print(f"  {labels[row['risk_level']]:>12}: {row['n']} countries")

        # Verify foreign keys
        fk_check = conn.execute("PRAGMA foreign_key_check;").fetchall()
        if fk_check:
            print(f"\n[WARN] Foreign key violations: {len(fk_check)}")
        else:
            print("\n[OK] No foreign key violations.")

        # Verify integrity
        ic = conn.execute("PRAGMA integrity_check;").fetchone()[0]
        print(f"[OK] Integrity check: {ic}")

    finally:
        conn.close()
