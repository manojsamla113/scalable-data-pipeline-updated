"""Generate a large synthetic CSV dataset in bounded-size batches."""
import argparse
from pathlib import Path
import pandas as pd


def generate_dataset(output_path, rows, batch_size=10000):
    if rows < 0:
        raise ValueError("rows cannot be negative")
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    first_batch = True
    for start in range(0, rows, batch_size):
        count = min(batch_size, rows - start)
        ids = range(start, start + count)
        frame = pd.DataFrame({
            "transaction_id": [f"TXN{i:09d}" for i in ids],
            "customer_id": [f"C{i % 1000 + 1:04d}" for i in range(start, start + count)],
            "amount": [float(i % 1000 + 1) for i in range(start, start + count)],
            "status": [["SUCCESS", "PENDING", "FAILED"][i % 3] for i in range(start, start + count)],
            "event_timestamp": ["2026-10-30T10:00:00Z"] * count,
        })
        frame.to_csv(
            output_path,
            mode="w" if first_batch else "a",
            index=False,
            header=first_batch,
            encoding="utf-8",
        )
        first_batch = False

    if rows == 0:
        output_path.write_text(
            "transaction_id,customer_id,amount,status,event_timestamp\n",
            encoding="utf-8",
        )
    print(f"Generated {rows:,} records at {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--output", default="data/input/transactions.csv")
    parser.add_argument("--batch-size", type=int, default=10000)
    args = parser.parse_args()
    generate_dataset(args.output, args.rows, args.batch_size)
