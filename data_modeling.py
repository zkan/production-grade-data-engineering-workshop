import pandas as pd

from datetime import datetime


def create_raw_layer(df: pd.DataFrame) -> pd.DataFrame:
    """
    Raw layer: store everything as-is. Never modify.
    Add ingestion metadata but do NOT clean or filter.
    """
    raw = df.copy()
    raw["_ingested_at"] = datetime.now()
    raw["_source"] = "orders_api"
    return raw


# Load data
df = pd.read_csv("orders.csv")

raw_layer = create_raw_layer(df)
print("Raw layer columns:", raw_layer.columns.tolist())
print(raw_layer)


def create_clean_layer(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean layer: deduplicated, validated, normalized.
    Still row-level — one row per order.
    """
    clean = raw_df.copy()

    # Deduplicate
    clean = clean.drop_duplicates(subset=["order_id"], keep="first")

    # Normalize nulls
    clean["country"] = clean["country"].fillna("UNKNOWN")

    # Explicit type casting
    clean["amount"] = pd.to_numeric(clean["amount"], errors="coerce")
    clean["event_time"] = pd.to_datetime(clean["event_time"], utc=False)

    # Add partition date from event_time
    clean["partition_date"] = clean["event_time"].dt.date.astype(str)

    return clean[["order_id", "customer_id", "country",
                  "amount", "status", "event_time", "partition_date"]]


clean_layer = create_clean_layer(raw_layer)
print(clean_layer.to_string())


def create_daily_revenue_mart(clean_df: pd.DataFrame) -> pd.DataFrame:
    """
    Mart layer: aggregated daily revenue.
    Grain: one row per (partition_date, country)
    
    Metric definition:
    - revenue = SUM(amount) WHERE status = 'completed'
    - refunds are EXCLUDED (tracked separately)
    - cancelled orders do NOT count
    """
    revenue = (
        clean_df[clean_df["status"] == "completed"]
        .groupby(["partition_date", "country"])
        .agg(
            order_count=("order_id", "count"),
            total_revenue=("amount", "sum"),
        )
        .reset_index()
    )
    return revenue


mart = create_daily_revenue_mart(clean_layer)
print("Daily Revenue Mart:")
print(mart.to_string())


import json


SCHEMA_CONTRACT = {
    "table": "fact_daily_revenue",
    "grain": "One row per (partition_date, country)",
    "primary_key": ["partition_date", "country"],
    "columns": {
        "partition_date": {"type": "date",   "nullable": False},
        "country":        {"type": "string", "nullable": False, "note": "UNKNOWN if null in source"},
        "order_count":    {"type": "int",    "nullable": False},
        "total_revenue":  {"type": "float",  "nullable": False, "note": "Only completed orders"},
    },
    "metric_rules": {
        "total_revenue": "SUM(amount) WHERE status = 'completed'. Refunds excluded."
    }
}
print(json.dumps(SCHEMA_CONTRACT, indent=2))
