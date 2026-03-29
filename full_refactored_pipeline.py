from dataclasses import dataclass

import numpy as np
import pandas as pd


# ── CONFIG ────────────────────────────────────────────────────
@dataclass
class PipelineConfig:
    """All configurable values in one place."""
    input_path:      str
    output_path:     str
    partition_date:  str
    primary_key:     str   = "order_id"
    status_filter:   str   = "completed"
    null_country_fill: str = "UNKNOWN"


# ── I/O Separation ────────────────────────────────────────────
def read_raw_orders(filepath: str) -> pd.DataFrame:
    """Read raw orders from CSV. Returns a DataFrame with original schema."""
    df = pd.read_csv(filepath)
    df["event_time"] = pd.to_datetime(df["event_time"])
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    return df


def write_revenue(df: pd.DataFrame, output_path: str) -> None:
    """Write final revenue table to CSV."""
    df.to_csv(output_path, index=False)
    print(f"Written: {output_path} ({len(df)} rows)")


# ── TRANSFORMATIONS (pure, testable) ──────────────────────────
def deduplicate(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Remove duplicates by key. Pure function."""
    before = len(df)
    result = df.drop_duplicates(subset=[key], keep="first")
    removed = before - len(result)
    if removed:
        print(f"  Deduplication: removed {removed} duplicate(s) on '{key}'")
    return result


def fill_nulls(df: pd.DataFrame, fill_map: dict) -> pd.DataFrame:
    """Fill nulls using a column→value mapping. Pure function."""
    return df.fillna(fill_map)


def filter_by_status(df: pd.DataFrame, status: str) -> pd.DataFrame:
    """Keep only rows matching the given status. Pure function."""
    result = df[df["status"] == status].copy()
    print(f"  Status filter '{status}': {len(df)} → {len(result)} rows")
    return result


def aggregate_revenue(df: pd.DataFrame, partition_date: str) -> pd.DataFrame:
    """
    Aggregate to daily revenue mart.
    Grain: one row per (partition_date, country).
    Revenue = SUM(amount) for completed orders.
    """
    return (
        df.groupby("country", as_index=False)
        .agg(order_count=("order_id", "count"),
             total_revenue=("amount", "sum"))
        .assign(partition_date=partition_date)
        [["partition_date", "country", "order_count", "total_revenue"]]
    )


# ── VALIDATION ────────────────────────────────────────────────
def validate_input(df: pd.DataFrame, required_cols: list) -> None:
    """Fail fast if input schema is wrong."""
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Input schema error — missing columns: {missing}")
    if df.empty:
        raise ValueError("Input DataFrame is empty.")
    print(f"  Input validation OK ({len(df)} rows, {len(df.columns)} columns)")


def validate_output(df: pd.DataFrame) -> None:
    """Sanity check on final output before publishing."""
    assert not df.empty,                             "Output is empty."
    assert df["total_revenue"].ge(0).all(),          "Negative revenue found."
    assert not df[["partition_date","country"]].duplicated().any(), "Duplicate primary key."
    print(f"  Output validation OK ({len(df)} rows)")


# ── ORCHESTRATOR ──────────────────────────────────────────────
def run_revenue_pipeline(config: PipelineConfig) -> pd.DataFrame:
    """
    Orchestrates the daily revenue pipeline.
    Sequencing only — all business logic lives in pure functions above.
    """
    print(f"\n{'─'*50}")
    print(f"Pipeline: daily_revenue | partition: {config.partition_date}")
    print('─'*50)

    raw = read_raw_orders(config.input_path)

    validate_input(raw, required_cols=[
        "order_id", "customer_id", "country", "amount", "status", "event_time"
    ])

    clean = (
        raw
        .pipe(deduplicate,   key=config.primary_key)
        .pipe(fill_nulls,    fill_map={"country": config.null_country_fill})
        .pipe(filter_by_status, status=config.status_filter)
    )

    output = aggregate_revenue(clean, config.partition_date)

    validate_output(output)

    write_revenue(output, config.output_path)
    print(f"\n✅ Pipeline complete.")

    return output


# ── RUN IT ────────────────────────────────────────────────────
config = PipelineConfig(
    input_path="orders.csv",
    output_path="revenue_2024-01-15.csv",
    partition_date="2024-01-15"
)
final_output = run_revenue_pipeline(config)
print("\nFinal Output:")
print(final_output.to_string(index=False))
