⚡ Scalable Data Pipeline

A practical Python data-engineering project for reliable, chunked data processing






Ingest → Validate → Transform → Publish → Reconcile

A runnable project demonstrating chunked CSV and JSON Lines (JSONL) ingestion, data-quality validation, duplicate prevention, SQLite publishing, rejected-record quarantine, readable console reporting, Excel export, automated tests, and performance benchmarking.

</div>

🌟 Highlights

Capability

What it does

📥 Chunked ingestion

Reads CSV input in configurable chunks; reads JSONL one line at a time in batches.

🧹 Data-quality checks

Validates required fields, amount, status, timestamp, and customer references.

🔁 Duplicate protection

Detects duplicate transaction IDs and avoids inserting existing target IDs again.

🗃️ SQLite publishing

Stores accepted transactions and pipeline-run metadata in SQLite.

🚧 Rejected-record quarantine

Captures source file, line number, original record, and rejection reason where available.

📊 Readable reporting

Prints run metrics and a rejected-record table in the terminal.

📗 Excel export

Creates a formatted .xlsx report with separate transaction columns.

🧪 Automated tests

Covers core processing, rejection, duplicate, rerun, and malformed-JSONL scenarios.

⏱️ Benchmarking

Compares runtime and sampled memory use across chunk sizes.

🧭 How the pipeline works

       CSV / JSONL input
               │
               ▼
      Chunked ingestion
               │
               ▼
      Data validation ───────────────┐
               │                     │
        ┌──────┴──────┐              ▼
        │             │       Rejected records
        ▼             ▼       CSV quarantine
  Valid records   Duplicate / invalid
        │
        ▼
   SQLite publishing
        │
        ▼
 Reconciliation + console report
        │
        └──────────────► Excel reject report (separate command)

🚀 Quick start (Windows / VS Code)

Run commands from the project root — the folder containing README.md, requirements.txt, src/, and tests/.

1. Create and activate a virtual environment

python -m venv .venv
.\.venv\Scripts\Activate.ps1

If PowerShell blocks activation, use the Python interpreter selected in VS Code and continue with the install command below.

2. Install dependencies

python -m pip install -r requirements.txt

3. Run the sample CSV pipeline

python -m src.pipeline --input data/input/transactions.csv --format csv --chunk-size 3

4. Run the JSONL pipeline

python -m src.pipeline --input data/input/transactions.jsonl --format jsonl --chunk-size 2

The console report includes the run status, input/accepted/rejected/duplicate/published counts, reconciliation status, a small valid-record preview, a rejected-record table, elapsed time, throughput, and output paths. The console preview is limited so the pipeline does not retain every record just for display.

📗 Export rejected records to Excel

Run the pipeline first so the reject CSV exists. Then run:

python -m src.export_rejects_excel

This creates data/rejects/rejected_records.xlsx with these columns:

Source File

Source Line

Transaction ID

Customer ID

Amount

Status

Rejection Reason

transactions.csv

5

T1004

C0004

-20.00

SUCCESS

Amount must be greater than zero

The example above illustrates the layout; actual rows and rejection reasons come from the input data. The workbook has a styled header, filters, wrapped text, and a frozen header row. Excel export is a separate step: run the export command again after processing new input to refresh the workbook.

To open the workbook on Windows:

Invoke-Item data\rejects\rejected_records.xlsx

🧪 Run the tests

python -m pytest -v

The current suite covers these core cases:

Valid CSV processing and reconciliation

Invalid-record quarantine

Duplicate transaction ID within an input file

Rerun without duplicate target rows

Malformed JSONL line rejection

A passing suite confirms these cases; it does not prove every possible malformed-input or recovery scenario is covered.

📈 Generate data and benchmark

Generate 100,000 transaction records

python -m src.generator --rows 100000 --output data/input/transactions.csv

⚠️ This command overwrites the CSV at the output path. Make a backup first if you want to preserve the small sample file.

The generator uses customer IDs C0001–C1000. Ensure data/input/customers.csv contains these IDs before processing the generated dataset. In PowerShell, regenerate the reference file with:

@("customer_id") + (1..1000 | ForEach-Object { "C{0:D4}" -f $_ }) | Set-Content -Encoding utf8 data/input/customers.csv

Compare chunk sizes

python -m src.benchmark --rows 100000 --chunk-sizes 1000 5000 10000

Benchmark result files are generated under reports/:

reports/benchmark_results.json

reports/benchmark_results.csv

Runtime and RSS measurements depend on the machine and are approximate local observations, not universal performance guarantees.

🗂️ Project structure

scalable-data-pipeline/
├── 📁 data/
│   ├── 📁 input/
│   │   ├── customers.csv
│   │   ├── transactions.csv
│   │   └── transactions.jsonl
│   ├── 📁 output/
│   │   └── transactions.db              # Created by the pipeline
│   └── 📁 rejects/
│       ├── rejected_records.csv          # Created/updated by the pipeline
│       └── rejected_records.xlsx         # Created by the Excel export command
├── 📁 reports/
│   ├── reconciliation.json              # Created by a pipeline run
│   ├── benchmark_results.json           # Created by a benchmark run
│   └── benchmark_results.csv            # Created by a benchmark run
├── 📁 src/
│   ├── __init__.py
│   ├── ingestion.py                     # CSV and JSONL readers
│   ├── validation.py                    # Transaction and reference validation
│   ├── storage.py                       # SQLite storage and duplicate protection
│   ├── pipeline.py                      # Pipeline orchestration and console report
│   ├── generator.py                     # Sample data generator
│   ├── benchmark.py                     # Performance benchmark utility
│   └── export_rejects_excel.py           # Formatted Excel export
├── 📁 tests/
│   └── test_pipeline.py                 # Automated pipeline tests
├── .gitignore
├── config.json
├── requirements.txt
└── README.md

Generated outputs may be absent until the relevant pipeline, export, or benchmark command is run. src/ also contains __init__.py, which marks it as a Python package.

✅ Validation rules

Required fields: transaction_id, customer_id, amount, status, and event_timestamp.

amount must be numeric and greater than zero.

status must be SUCCESS, PENDING, or FAILED.

event_timestamp must be parseable as a date/time.

customer_id must exist in data/input/customers.csv.

Duplicate transaction IDs within the input are rejected.

The SQLite target prevents an existing transaction ID from being inserted again.

📦 Outputs at a glance

Path

Purpose

Created by

data/output/transactions.db

Published transactions and run metadata

Pipeline

data/rejects/rejected_records.csv

Rejected records and rejection details

Pipeline

data/rejects/rejected_records.xlsx

Human-readable Excel report

Excel export command

reports/reconciliation.json

Run metrics and reconciliation result

Pipeline

reports/benchmark_results.csv

Benchmark results in CSV

Benchmark command

reports/benchmark_results.json

Benchmark results in JSON

Benchmark command

🔄 Reconciliation and idempotency

The pipeline reconciles the records it counts using:

input_records = accepted_records + rejected_records

published_records counts new database inserts, not every valid record. After a successful first run, rerunning the same input may show published_records: 0 because those transaction IDs already exist. This is expected idempotent behavior.

🧠 Memory behavior and known limitations

CSV input is read in pandas chunks; JSONL is read from individual lines in batches.

Duplicate IDs for the current run are tracked in SQLite rather than retaining every ID in a Python set.

Memory use still depends on the current CSV chunk and the customer reference data size.

Malformed JSONL lines are quarantined individually.

Malformed CSV parsing errors currently fail the run rather than being quarantined individually.

SQLite writes commit by chunk. If a later chunk fails, earlier committed chunks may remain in the target database.

Benchmark RSS values are sampled and approximate; they are not a formal peak-memory guarantee.

🛡️ Before publishing to GitHub

Run python -m pytest -v and review the results.

Run the sample CSV pipeline.

Run python -m src.export_rejects_excel and open the workbook.

Built with Python · Pandas · SQLite · Pytest · OpenPyXL

Review .gitignore; do not commit secrets, virtual environments, or large generated datasets.

Check that any benchmark numbers in your submission were measured on your own machine.
