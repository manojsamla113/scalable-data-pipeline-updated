"""Chunked CSV and JSON Lines readers."""
import json
import pandas as pd


def read_csv_in_chunks(file_path, chunk_size=5000):
    """Yield CSV DataFrames without loading the complete file into memory."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero")

    yield from pd.read_csv(
        file_path,
        chunksize=chunk_size,
        dtype="string",
        keep_default_na=False,
        on_bad_lines="error",
        encoding="utf-8-sig",
    )


def read_jsonl_in_chunks(file_path, chunk_size=5000):
    """
    Yield (dataframe, errors) batches from JSON Lines.

    Errors are yielded immediately so malformed lines cannot accumulate
    in an unbounded in-memory list.
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero")

    batch = []

    with open(file_path, "r", encoding="utf-8-sig") as handle:
        for line_number, line in enumerate(handle, start=1):
            raw = line.rstrip("\r\n")

            if not raw.strip():
                yield pd.DataFrame(), [{
                    "source_line": line_number,
                    "raw_record": raw,
                    "rejection_reason": "Empty JSON line",
                }]
                continue

            try:
                record = json.loads(raw)
                if not isinstance(record, dict):
                    raise ValueError("Each JSONL record must be a JSON object")
                record["_source_line"] = line_number
                batch.append(record)
            except (json.JSONDecodeError, ValueError) as exc:
                yield pd.DataFrame(), [{
                    "source_line": line_number,
                    "raw_record": raw,
                    "rejection_reason": f"Malformed JSON: {exc}",
                }]
                continue

            if len(batch) >= chunk_size:
                yield pd.DataFrame(batch), []
                batch = []

    if batch:
        yield pd.DataFrame(batch), []
