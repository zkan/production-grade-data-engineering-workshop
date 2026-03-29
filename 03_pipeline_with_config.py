from dataclasses import dataclass

import pandas as pd


@dataclass
class PipelineConfig:
    """All configurable values in one place."""
    input_path:      str
    output_path:     str
    partition_date:  str
    primary_key:     str   = "order_id"
    status_filter:   str   = "completed"
    null_country_fill: str = "UNKNOWN"


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


def run_pipeline_with_config(config: PipelineConfig) -> pd.DataFrame:
    """
    Pipeline driven entirely by config — no hardcoded values inside.
    """
    raw = read_raw_orders(config.input_path)

    clean = (
        raw
        .drop_duplicates(subset=[config.primary_key], keep="first")
        .assign(country=lambda df: df["country"].fillna(config.null_country_fill))
    )

    revenue = (
        clean[clean["status"] == config.status_filter]
        .groupby("country", as_index=False)
        .agg(order_count=("order_id", "count"),
             total_revenue=("amount", "sum"))
        .assign(partition_date=config.partition_date)
    )

    write_revenue(revenue, config.output_path)
    print(f"\n✅ Pipeline complete.")

    return revenue


config = PipelineConfig(
    input_path="orders.csv",
    output_path="revenue_2024-01-15.csv",
    partition_date="2024-01-15"
)
revenue = run_pipeline_with_config(config)
print(revenue)
print("done")
