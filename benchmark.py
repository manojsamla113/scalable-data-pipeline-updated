"""Simple benchmark with process RSS sampling."""
import argparse
import json
import os
from pathlib import Path
import threading
import time

import pandas as pd
import psutil

from src.generator import generate_dataset
from src.pipeline import run_pipeline


def benchmark_one(input_path, chunk_size):
    process = psutil.Process(os.getpid())
    stop = threading.Event()
    peak = {"rss": process.memory_info().rss}

    def sample_memory():
        while not stop.is_set():
            try:
                peak["rss"] = max(peak["rss"], process.memory_info().rss)
            except psutil.Error:
                break
            time.sleep(0.02)

    monitor = threading.Thread(target=sample_memory, daemon=True)
    monitor.start()
    start = time.perf_counter()
    try:
        report = run_pipeline(
            input_path, "csv", chunk_size,
            database_path=f"data/output/benchmark_{chunk_size}.db",
            rejects_path=f"data/rejects/rejects_{chunk_size}.csv",
            report_path=f"reports/reconciliation_{chunk_size}.json",
        )
    finally:
        stop.set()
        monitor.join()

    elapsed = time.perf_counter() - start
    return {
        "chunk_size": chunk_size,
        "input_records": report["input_records"],
        "accepted_records": report["accepted_records"],
        "rejected_records": report["rejected_records"],
        "published_records": report["published_records"],
        "elapsed_seconds": round(elapsed, 3),
        "records_per_second": round(report["input_records"] / elapsed, 2) if elapsed else 0,
        "peak_rss_mb": round(peak["rss"] / (1024 * 1024), 2),
        "status": report["status"],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--chunk-sizes", type=int, nargs="+", default=[1000, 5000, 10000])
    args = parser.parse_args()

    if args.rows <= 0 or any(size <= 0 for size in args.chunk_sizes):
        parser.error("rows and all chunk sizes must be positive")

    customers_path = Path("data/input/customers.csv")
    customers_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({
        "customer_id": [f"C{i:04d}" for i in range(1, 1001)]
    }).to_csv(customers_path, index=False)

    input_path = "data/input/benchmark_transactions.csv"
    generate_dataset(input_path, args.rows)

    results = [benchmark_one(input_path, size) for size in args.chunk_sizes]
    Path("reports").mkdir(exist_ok=True)
    Path("reports/benchmark_results.json").write_text(
        json.dumps(results, indent=2), encoding="utf-8"
    )
    pd.DataFrame(results).to_csv("reports/benchmark_results.csv", index=False)
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
