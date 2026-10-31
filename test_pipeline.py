import json
import sqlite3

import pandas as pd

from src.pipeline import run_pipeline


def setup_project(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for folder in ["data/input", "data/output", "data/rejects", "reports"]:
        (tmp_path / folder).mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"customer_id": ["C001", "C002"]}).to_csv(
        "data/input/customers.csv", index=False
    )


def test_csv_valid_records_and_reconciliation(tmp_path, monkeypatch):
    setup_project(tmp_path, monkeypatch)
    pd.DataFrame([
        {"transaction_id": "T001", "customer_id": "C001", "amount": 100,
         "status": "SUCCESS", "event_timestamp": "2026-10-30T10:00:00Z"},
        {"transaction_id": "T002", "customer_id": "C002", "amount": 200,
         "status": "PENDING", "event_timestamp": "2026-10-30T11:00:00Z"},
    ]).to_csv("data/input/transactions.csv", index=False)

    report = run_pipeline("data/input/transactions.csv", "csv", 1)
    assert report["input_records"] == 2
    assert report["accepted_records"] == 2
    assert report["rejected_records"] == 0
    assert report["published_records"] == 2
    assert report["reconciliation_passed"] is True


def test_invalid_record_is_quarantined(tmp_path, monkeypatch):
    setup_project(tmp_path, monkeypatch)
    pd.DataFrame([
        {"transaction_id": "T001", "customer_id": "C001", "amount": -1,
         "status": "SUCCESS", "event_timestamp": "2026-10-30T10:00:00Z"}
    ]).to_csv("data/input/transactions.csv", index=False)

    report = run_pipeline("data/input/transactions.csv", "csv", 1)
    assert report["rejected_records"] == 1
    assert report["accepted_records"] == 0
    assert report["reconciliation_passed"] is True
    rejected = pd.read_csv("data/rejects/rejected_records.csv")
    assert len(rejected) == 1
    assert "Amount must be numeric" in rejected.loc[0, "rejection_reason"]


def test_duplicate_within_file_is_rejected(tmp_path, monkeypatch):
    setup_project(tmp_path, monkeypatch)
    record = {"transaction_id": "T001", "customer_id": "C001", "amount": 100,
              "status": "SUCCESS", "event_timestamp": "2026-10-30T10:00:00Z"}
    pd.DataFrame([record, record]).to_csv("data/input/transactions.csv", index=False)

    report = run_pipeline("data/input/transactions.csv", "csv", 1)
    assert report["input_records"] == 2
    assert report["accepted_records"] == 1
    assert report["duplicate_records"] == 1
    assert report["rejected_records"] == 1
    rejected = pd.read_csv("data/rejects/rejected_records.csv")
    assert len(rejected) == 1
    assert rejected.loc[0, "rejection_reason"] == "Duplicate transaction ID within input"
    assert int(rejected.loc[0, "source_line"]) == 3


def test_rerun_does_not_duplicate_target_rows(tmp_path, monkeypatch):
    setup_project(tmp_path, monkeypatch)
    pd.DataFrame([
        {"transaction_id": "T001", "customer_id": "C001", "amount": 100,
         "status": "SUCCESS", "event_timestamp": "2026-10-30T10:00:00Z"}
    ]).to_csv("data/input/transactions.csv", index=False)

    first = run_pipeline("data/input/transactions.csv", "csv", 1)
    second = run_pipeline("data/input/transactions.csv", "csv", 1)
    assert first["published_records"] == 1
    assert second["published_records"] == 0

    with sqlite3.connect("data/output/transactions.db") as conn:
        assert conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 1


def test_jsonl_malformed_line_is_rejected(tmp_path, monkeypatch):
    setup_project(tmp_path, monkeypatch)
    path = "data/input/transactions.jsonl"
    with open(path, "w", encoding="utf-8") as file:
        file.write(json.dumps({
            "transaction_id": "T001", "customer_id": "C001", "amount": 100,
            "status": "SUCCESS", "event_timestamp": "2026-10-30T10:00:00Z"
        }) + "\n")
        file.write('{"transaction_id": broken}\n')

    report = run_pipeline(path, "jsonl", 1)
    assert report["input_records"] == 2
    assert report["accepted_records"] == 1
    assert report["rejected_records"] == 1
    assert report["reconciliation_passed"] is True
