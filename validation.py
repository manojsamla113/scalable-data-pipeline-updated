"""Record-level schema and data-quality validation."""
import pandas as pd

REQUIRED_COLUMNS = [
    "transaction_id",
    "customer_id",
    "amount",
    "status",
    "event_timestamp",
]
ALLOWED_STATUSES = {"SUCCESS", "PENDING", "FAILED"}


def validate_chunk(df, valid_customer_ids=None, allowed_statuses=None):
    """Return (accepted_df, rejected_df). Extra input columns are ignored."""
    allowed_statuses = allowed_statuses or ALLOWED_STATUSES
    missing_columns = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing_columns:
        raise ValueError(
            "Missing required columns: " + ", ".join(sorted(missing_columns))
        )

    working = df[REQUIRED_COLUMNS].copy()
    working["original_record"] = df[REQUIRED_COLUMNS].to_dict(orient="records")
    working["rejection_reason"] = ""

    for column in ["transaction_id", "customer_id", "status", "event_timestamp"]:
        values = working[column].astype("string").str.strip()
        missing = values.isna() | values.eq("")
        working.loc[missing, "rejection_reason"] += f"Missing {column}; "
        working[column] = values

    working["status"] = working["status"].str.upper()
    invalid_status = ~working["status"].isin(allowed_statuses)
    working.loc[invalid_status, "rejection_reason"] += "Invalid transaction status; "

    amounts = pd.to_numeric(working["amount"], errors="coerce")
    invalid_amount = amounts.isna() | (amounts <= 0)
    working.loc[invalid_amount, "rejection_reason"] += (
        "Amount must be numeric and greater than zero; "
    )
    working["amount"] = amounts

    timestamps = pd.to_datetime(
        working["event_timestamp"], errors="coerce", utc=True
    )
    invalid_timestamp = timestamps.isna()
    working.loc[invalid_timestamp, "rejection_reason"] += (
        "Invalid event timestamp; "
    )
    working["event_timestamp"] = timestamps

    if valid_customer_ids is not None:
        invalid_customer = ~working["customer_id"].isin(valid_customer_ids)
        working.loc[invalid_customer, "rejection_reason"] += (
            "Customer reference does not exist; "
        )

    accepted_mask = working["rejection_reason"].eq("")
    accepted = working.loc[accepted_mask].copy()
    rejected = working.loc[~accepted_mask].copy()
    return accepted, rejected
