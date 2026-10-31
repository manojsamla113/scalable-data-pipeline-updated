"""Command-line chunked CSV/JSONL processing pipeline."""
import argparse
import csv
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.ingestion import read_csv_in_chunks, read_jsonl_in_chunks
from src.storage import (
    finish_run,
    initialize_database,
    publish_chunk,
    start_run,
)
from src.validation import validate_chunk

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

REJECT_COLUMNS = [
    "source_file", "source_line", "original_record", "rejection_reason"
]


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def append_rejects(path, rows):
    if not rows:
        return
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=REJECT_COLUMNS)
        if write_header:
            writer.writeheader()
        for row in rows:
            writer.writerow({
                "source_file": row.get("source_file", ""),
                "source_line": row.get("source_line", ""),
                "original_record": json.dumps(
                    row.get("original_record", row.get("raw_record", "")),
                    ensure_ascii=False,
                    default=str,
                ),
                "rejection_reason": row.get("rejection_reason", ""),
            })


def rejected_rows_from_frame(df, source_file, line_numbers=None):
    rows = []
    for position, (_, row) in enumerate(df.iterrows()):
        rows.append({
            "source_file": str(source_file),
            "source_line": (
                line_numbers[position]
                if line_numbers is not None and position < len(line_numbers)
                else ""
            ),
            "original_record": row.get("original_record", {}),
            "rejection_reason": str(row.get("rejection_reason", "")),
        })
    return rows


def run_pipeline(input_path, file_format="csv", chunk_size=5000,
                 database_path="data/output/transactions.db",
                 customers_path="data/input/customers.csv",
                 rejects_path="data/rejects/rejected_records.csv",
                 report_path="reports/reconciliation.json"):
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero")
    if file_format not in {"csv", "jsonl"}:
        raise ValueError("file_format must be 'csv' or 'jsonl'")

    input_path = Path(input_path)
    if not input_path.is_file():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    customers_path = Path(customers_path)
    if not customers_path.is_file():
        raise FileNotFoundError(f"Customer reference file not found: {customers_path}")

    customers = pd.read_csv(
        customers_path, dtype="string", keep_default_na=False, encoding="utf-8-sig"
    )
    if "customer_id" not in customers.columns:
        raise ValueError("Customer reference file must contain customer_id")

    customer_ids = customers["customer_id"].str.strip()
    if customer_ids.eq("").any():
        raise ValueError("Customer reference contains empty customer IDs")
    if customer_ids.duplicated().any():
        raise ValueError("Customer reference contains duplicate customer IDs")
    valid_customer_ids = set(customer_ids.tolist())

    database_path = Path(database_path)
    rejects_path = Path(rejects_path)
    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    rejects_path.parent.mkdir(parents=True, exist_ok=True)

    run_id = str(uuid.uuid4())
    started_at = utc_now()
    started_clock = time.perf_counter()
    metrics = {
        "input_records": 0,
        "accepted_records": 0,
        "rejected_records": 0,
        "duplicate_records": 0,
        "published_records": 0,
    }

    initialize_database(str(database_path))
    start_run(str(database_path), run_id, str(input_path), started_at)
    rejects_path.write_text("", encoding="utf-8")

    try:
        if file_format == "csv":
            batches = (
                (chunk, [])
                for chunk in read_csv_in_chunks(input_path, chunk_size)
            )
        else:
            batches = read_jsonl_in_chunks(input_path, chunk_size)

        csv_source_line = 2  # Header is line 1.
        for batch, parse_errors in batches:
            if not batch.empty:
                if "_source_line" in batch.columns:
                    source_lines = batch["_source_line"].tolist()
                    batch = batch.drop(columns=["_source_line"])
                elif file_format == "csv":
                    source_lines = list(
                        range(csv_source_line, csv_source_line + len(batch))
                    )
                    csv_source_line += len(batch)
                else:
                    source_lines = None

                metrics["input_records"] += len(batch)
                accepted, rejected = validate_chunk(
                    batch, valid_customer_ids=valid_customer_ids
                )

                source_line_by_index = (
                    dict(zip(batch.index.tolist(), source_lines))
                    if source_lines is not None else {}
                )
                rejected_line_numbers = [
                    source_line_by_index.get(index, "")
                    for index in rejected.index.tolist()
                ]
                rejected_rows = rejected_rows_from_frame(
                    rejected, input_path, rejected_line_numbers
                )
                metrics["rejected_records"] += len(rejected)

                duplicates, published = publish_chunk(
                    str(database_path), run_id, accepted
                )

                if duplicates:
                    # publish_chunk returns one ID for each rejected occurrence.
                    # Select only those occurrences, not the first valid occurrence
                    # when duplicate IDs appear more than once in the same chunk.
                    duplicate_counts = {}
                    for transaction_id in duplicates:
                        duplicate_counts[transaction_id] = (
                            duplicate_counts.get(transaction_id, 0) + 1
                        )
                    duplicate_indexes = []
                    for transaction_id, group in accepted.groupby(
                        "transaction_id", sort=False
                    ):
                        count = duplicate_counts.get(str(transaction_id), 0)
                        if count:
                            duplicate_indexes.extend(group.index.tolist()[-count:])
                    duplicate_df = accepted.loc[duplicate_indexes].copy()
                    duplicate_df["rejection_reason"] = (
                        "Duplicate transaction ID within input"
                    )
                    duplicate_line_numbers = [
                        source_line_by_index.get(index, "")
                        for index in duplicate_df.index.tolist()
                    ]
                    rejected_rows.extend(
                        rejected_rows_from_frame(
                            duplicate_df, input_path, duplicate_line_numbers
                        )
                    )
                    metrics["duplicate_records"] += len(duplicates)
                    metrics["rejected_records"] += len(duplicates)

                metrics["accepted_records"] += len(accepted) - len(duplicates)
                metrics["published_records"] += published
                append_rejects(rejects_path, rejected_rows)

            for error in parse_errors:
                metrics["input_records"] += 1
                metrics["rejected_records"] += 1
                append_rejects(rejects_path, [{
                    "source_file": str(input_path),
                    "source_line": error["source_line"],
                    "raw_record": error["raw_record"],
                    "rejection_reason": error["rejection_reason"],
                }])

        elapsed = time.perf_counter() - started_clock
        finished_at = utc_now()
        report = {
            "run_id": run_id,
            "source_file": str(input_path),
            "format": file_format,
            "chunk_size": chunk_size,
            "started_at": started_at,
            "finished_at": finished_at,
            **metrics,
            "elapsed_seconds": round(elapsed, 3),
            "records_per_second": round(
                metrics["input_records"] / elapsed, 2
            ) if elapsed > 0 else 0,
            "reconciliation_passed": (
                metrics["input_records"]
                == metrics["accepted_records"] + metrics["rejected_records"]
            ),
            "status": "SUCCESS",
        }

        finish_run(
            str(database_path), run_id, metrics, finished_at, "SUCCESS"
        )
        report_path.write_text(
            json.dumps(report, indent=2), encoding="utf-8"
        )
        logging.info("Pipeline completed: %s", json.dumps(report))
        return report

    except Exception as exc:
        finished_at = utc_now()
        finish_run(
            str(database_path), run_id, metrics, finished_at, "FAILED", str(exc)
        )
        failed_report = {
            "run_id": run_id,
            "source_file": str(input_path),
            "format": file_format,
            "started_at": started_at,
            "finished_at": finished_at,
            **metrics,
            "status": "FAILED",
            "failure_reason": str(exc),
        }
        report_path.write_text(
            json.dumps(failed_report, indent=2), encoding="utf-8"
        )
        logging.exception("Pipeline failed")
        raise


def main():
    parser = argparse.ArgumentParser(description="Chunked CSV/JSONL data pipeline")
    parser.add_argument("--input", required=True, help="Input CSV or JSONL path")
    parser.add_argument("--format", choices=["csv", "jsonl"], required=True)
    parser.add_argument("--chunk-size", type=int, default=5000)
    args = parser.parse_args()
    run_pipeline(args.input, args.format, args.chunk_size)


if __name__ == "__main__":
    main()
