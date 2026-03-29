import pandas as pd


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


df = read_raw_orders("orders.csv")

# Clean and compute
df2 = df.drop_duplicates()
df3 = df2[df2["status"] == "completed"]
df4 = df3.copy()
df4["amount"] = pd.to_numeric(df4["amount"], errors="coerce")
df4["country"] = df4["country"].fillna("UNKNOWN")
result = df4.groupby("country")["amount"].sum().reset_index()
result.columns = ["country", "revenue"]
result["run_date"] = "2024-01-15"

write_revenue(result, "revenue_2024-01-15.csv")
print("done")
