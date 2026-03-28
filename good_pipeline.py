import pandas as pd
import numpy as np


def deduplicate(df, primary_key: str) -> pd.DataFrame:
    """
    Remove duplicate rows, keeping the first occurrence.
    Safe to call multiple times — always produces the same result.
    """
    before = len(df)
    df_deduped = df.drop_duplicates(subset=[primary_key], keep="first")
    after = len(df_deduped)
    print(f"Deduplication: {before} → {after} rows (removed {before - after})")
    return df_deduped


# Load data
df = pd.read_csv("orders.csv")

clean_df = deduplicate(df, primary_key="order_id")
print(clean_df)


def load_partition(df: pd.DataFrame, partition_date: str, output: dict):
    """
    Overwrite the partition for this date.
    Running twice on the same date produces the same result — no duplicates.
    """
    df["partition_date"] = partition_date

    # Remove existing data for this partition (simulate overwrite)
    output.pop(partition_date, None)

    # Write new data
    output[partition_date] = df.copy()
    print(f"Partition '{partition_date}' loaded: {len(df)} rows")


# Simulated output storage (in practice: a table or file)
output_store = {}

load_partition(clean_df, "2024-01-15", output_store)
load_partition(clean_df, "2024-01-15", output_store)  # Re-run: safe!

print("Partitions in store:", list(output_store.keys()))
print("Row count:", len(output_store["2024-01-15"]))  # Still correct


def convert_event_time_to_datetime(df: pd.DataFrame):
    df["event_time"] = pd.to_datetime(df["event_time"])
    return df


clean_df = convert_event_time_to_datetime(clean_df)


def filter_by_event_time(df: pd.DataFrame, partition_date: str) -> pd.DataFrame:
    """
    Only include records that BELONG to this partition date by event_time.
    Late-arriving records will be caught on backfill.
    """
    target = pd.Timestamp(partition_date)
    filtered = df[df["event_time"].dt.normalize() == target]
    late = df[df["event_time"].dt.normalize() < target]
    
    if not late.empty:
        print(f"WARNING: {len(late)} late record(s) found — will be excluded from {partition_date}:")
        print(late[["order_id", "event_time"]])
    
    return filtered


on_time_df = filter_by_event_time(clean_df, "2024-01-15")
print(f"\nOn-time records: {len(on_time_df)}")


def validate_schema(df: pd.DataFrame, required_columns: list, required_types: dict):
    """
    Check schema before transformation begins.
    Raises an error early rather than silently producing wrong output.
    """
    print("=== Schema Validation ===")
    
    # Check required columns exist
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    # Check types
    for col, expected_type in required_types.items():
        actual = df[col].dtype
        if not np.issubdtype(actual, expected_type):
            raise TypeError(f"Column '{col}': expected {expected_type}, got {actual}")
    
    print("Schema OK ✓")


validate_schema(
    clean_df,
    required_columns=["order_id", "customer_id", "amount", "event_time"],
    required_types={"amount": np.number, "event_time": np.datetime64}
)


import functools
import time


def retry(func, *args, max_attempts=3, delay_seconds=30, backoff=2.0, **kwargs):
    delay = delay_seconds
    for attempt in range(1, max_attempts + 1):
        try:
            return func(*args, **kwargs)
        except (ConnectionError, TimeoutError, TypeError) as e:
            if attempt == max_attempts:
                raise
            print(f"Attempt {attempt} failed: {e} — retrying in {delay}s")
            time.sleep(delay)
            delay *= backoff


def with_retry(max_attempts=3, delay_seconds=30, backoff=2.0):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return retry(func, *args, max_attempts=max_attempts,
                         delay_seconds=delay_seconds, backoff=backoff, **kwargs)
        return wrapper
    return decorator


@with_retry(max_attempts=3, delay_seconds=5, backoff=2.0)
def reliable_pipeline(raw_df: pd.DataFrame, partition_date: str, output: dict):
    """
    Production-grade pipeline:
    1. Validate schema
    2. Deduplicate
    3. Filter by event time
    4. Transform
    5. Overwrite partition (idempotent write)
    """
    print(f"\n🚀 Running pipeline for: {partition_date}")

    # Step 0: Fix data type issues
    raw_df = convert_event_time_to_datetime(raw_df)
    
    # Step 1: Fail fast on schema issues
    validate_schema(
        raw_df,
        required_columns=["order_id", "customer_id", "amount", "event_time", "status"],
        required_types={"amount": np.number, "event_time": np.datetime64}
    )
    
    # Step 2: Deduplicate
    deduped = deduplicate(raw_df, primary_key="order_id")
    
    # Step 3: Filter to this partition
    filtered = filter_by_event_time(deduped, partition_date)
    
    # Step 4: Transform — only completed orders count as revenue
    revenue_df = (
        filtered[filtered["status"] == "completed"]
        .assign(partition_date=partition_date)
        [["partition_date", "order_id", "customer_id", "country", "amount"]]
    )
    
    # Step 5: Overwrite partition
    load_partition(revenue_df, partition_date, output)
    
    print(f"✅ Pipeline complete. {len(revenue_df)} revenue records written.")
    return revenue_df


output_store = {}
result = reliable_pipeline(df, "2024-01-15", output_store)
print(result)


def run_backfill(raw_df: pd.DataFrame, start_date: str, end_date: str, output: dict):
    """
    Safely re-run the pipeline for a date range.
    Each partition is independent — safe to rerun any date.
    """
    dates = pd.date_range(start=start_date, end=end_date, freq="D")
    print(f"Backfilling {len(dates)} days: {start_date} → {end_date}\n")
    
    for date in dates:
        partition_date = date.strftime("%Y-%m-%d")
        try:
            reliable_pipeline(raw_df, partition_date, output)
        except Exception as e:
            print(f"⚠️  Partition {partition_date} failed: {e}")


run_backfill(df, "2024-01-10", "2024-01-15", output_store)
print("\nFinal partitions:", list(output_store.keys()))

print("\nFinal data:")
for key, value in output_store.items(): 
    print("=" * 50)
    print(key)
    print(value)
