"""SQLite storage, run tracking, and disk-backed duplicate detection."""
import sqlite3
from pathlib import Path


def connect(db_path):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def initialize_database(db_path):
    with connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                amount REAL NOT NULL CHECK (amount > 0),
                status TEXT NOT NULL,
                event_timestamp TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_ids (
                run_id TEXT NOT NULL,
                transaction_id TEXT NOT NULL,
                PRIMARY KEY (run_id, transaction_id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id TEXT PRIMARY KEY,
                source_file TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                input_records INTEGER NOT NULL DEFAULT 0,
                accepted_records INTEGER NOT NULL DEFAULT 0,
                rejected_records INTEGER NOT NULL DEFAULT 0,
                duplicate_records INTEGER NOT NULL DEFAULT 0,
                published_records INTEGER NOT NULL DEFAULT 0,
                failure_reason TEXT
            )
        """)


def start_run(db_path, run_id, source_file, started_at):
    with connect(db_path) as conn:
        conn.execute("""
            INSERT INTO pipeline_runs (run_id, source_file, started_at, status)
            VALUES (?, ?, ?, 'RUNNING')
        """, (run_id, source_file, started_at))


def publish_chunk(db_path, run_id, accepted_df):
    """
    Atomically claim IDs for this run and insert accepted rows.

    IDs repeated within the same input run are returned as duplicates.
    Existing IDs from earlier runs are safely ignored by the target primary key.
    """
    duplicates = []
    published = 0

    with connect(db_path) as conn:
        # The connection context commits all claims/inserts together.
        for row in accepted_df.itertuples(index=False):
            cursor = conn.execute("""
                INSERT INTO processed_ids (run_id, transaction_id)
                VALUES (?, ?)
                ON CONFLICT(run_id, transaction_id) DO NOTHING
            """, (run_id, str(row.transaction_id)))

            if cursor.rowcount == 0:
                duplicates.append(str(row.transaction_id))
                continue

            result = conn.execute("""
                INSERT INTO transactions (
                    transaction_id, customer_id, amount, status, event_timestamp
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(transaction_id) DO NOTHING
            """, (
                str(row.transaction_id),
                str(row.customer_id),
                float(row.amount),
                str(row.status),
                row.event_timestamp.isoformat(),
            ))
            published += result.rowcount

    return duplicates, published


def finish_run(db_path, run_id, metrics, finished_at, status, failure_reason=None):
    with connect(db_path) as conn:
        conn.execute("""
            UPDATE pipeline_runs
            SET finished_at = ?,
                status = ?,
                input_records = ?,
                accepted_records = ?,
                rejected_records = ?,
                duplicate_records = ?,
                published_records = ?,
                failure_reason = ?
            WHERE run_id = ?
        """, (
            finished_at,
            status,
            metrics.get("input_records", 0),
            metrics.get("accepted_records", 0),
            metrics.get("rejected_records", 0),
            metrics.get("duplicate_records", 0),
            metrics.get("published_records", 0),
            failure_reason,
            run_id,
        ))
