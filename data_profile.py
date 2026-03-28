import pandas as pd


# Load data
df = pd.read_csv("orders.csv")


def profile_schema(df):
    print("=== Schema & Types ===")
    print(df.dtypes)
    print()
    print("Shape:", df.shape)


def profile_nulls(df):
    print("=== Null Distribution ===")
    null_counts = df.isnull().sum()
    null_percentage = (df.isnull().mean() * 100).round(1)
    result = pd.DataFrame({"null_count": null_counts, "null_%": null_percentage})
    print(result)


def profile_uniqueness(df, column):
    print(f"=== Uniqueness: {column} ===")
    total = len(df)
    unique = df[column].nunique()
    duplicates = total - unique
    print(f"Total rows   : {total}")
    print(f"Unique values: {unique}")
    print(f"Duplicates   : {duplicates}")
    if duplicates > 0:
        print()
        print("Duplicate rows:")
        print(df[df.duplicated(subset=[column], keep=False)])


def profile_distribution(df):
    print("=== Status Distribution ===")
    print(df["status"].value_counts())
    print()
    print("=== Amount Stats ===")
    print(df["amount"].describe())
    print()
    print("=== Negative Amounts ===")
    print(df[df["amount"] < 0])


def profile_temporal(df, pipeline_run_date: str):
    run_date = pd.Timestamp(pipeline_run_date)
    print("=== Temporal Patterns ===")
    df["event_time"] = pd.to_datetime(df["event_time"])
    df["days_late"] = (run_date - df["event_time"].dt.normalize()).dt.days
    print(df[["order_id", "event_time", "days_late"]].sort_values("days_late", ascending=False))
    print()
    print("Late records (>1 day):")
    print(df[df["days_late"] > 1][["order_id", "event_time", "days_late"]])


def profile_row_count(df: pd.DataFrame) -> None:
    print("=== Row & Partition Count ===")
    print(f"Total rows      : {len(df):>10,}")
    print(f"Total columns   : {len(df.columns):>10,}")
    print(f"Date time range : {df["event_time"].min()}  →  {df["event_time"].max()}")
    print(f"Partitions      : {df["event_time"].nunique():>10,} partitions")


def profile_partition_sizes(df: pd.DataFrame, partition_col: str = "event_time"):
    print(f"=== Partition Sizes (by {partition_col}) ===")
 
    sizes = (
        df.groupby(partition_col)
        .size()
        .reset_index(name="row_count")
        .sort_values(partition_col)
    )
 
    print(f"Summary:")
    print(f"  Min rows/partition : {sizes["row_count"].min():>8,}  ({sizes.loc[sizes["row_count"].idxmin(), partition_col]})")
    print(f"  Max rows/partition : {sizes["row_count"].max():>8,}  ({sizes.loc[sizes["row_count"].idxmax(), partition_col]})")
    print(f"  Avg rows/partition : {sizes["row_count"].mean():>8,.1f}")
    print(f"  Std deviation : {sizes["row_count"].std():>8,.1f}")


def profile_growth_rate(sizes: pd.DataFrame, partition_col: str = "event_time", window: int = 7):
    print(f"=== Growth Rate ===")

    sizes = (
        df.groupby(partition_col)
        .size()
        .reset_index(name="row_count")
        .sort_values(partition_col)
    ) 
    sizes = sizes.copy().sort_values("event_time").reset_index(drop=True)

    # Hour-over hour change
    sizes["prev_hour"]     = sizes["row_count"].shift(1)
    sizes["hoh_change"]   = sizes["row_count"] - sizes["prev_hour"]
    sizes["hoh_growth_%"] = (sizes["hoh_change"] / sizes["prev_hour"] * 100).round(1)
 
    # 7-hour rolling average (smooths out noise)
    sizes[f"{window}h_avg"] = sizes["row_count"].rolling(window).mean().round(1)
  
    print("Hour-over-Hour (last 10 hours):")
    print(
        sizes[["event_time", "row_count", "hoh_change", "hoh_growth_%", f"{window}h_avg"]]
        .tail(10)
        .to_string(index=False)
    )

    # Weekly totals
    sizes["week"] = pd.to_datetime(sizes["event_time"]).dt.to_period("W")
    weekly = (
        sizes.groupby("week")["row_count"]
        .sum()
        .reset_index(name="weekly_total")
    )
    weekly["wow_growth_%"] = (
        (weekly["weekly_total"] / weekly["weekly_total"].shift(1) - 1) * 100
    ).round(1)
 
    print("\nWeek-over-Week (WoW):")
    print(weekly.to_string(index=False))

    avg_wow = weekly["wow_growth_%"].mean()
    print(f"\nAverage WoW growth : {avg_wow:.1f}%")
    if avg_wow > 20:
        print("⚠️  High growth — review partitioning and storage strategy soon.")
    elif avg_wow > 0:
        print("✅ Steady growth — monitor monthly.")
    else:
        print("📉 Volume is flat or declining — verify upstream pipeline health.")  


def run_full_profile(df):
    print("=" * 50)
    profile_schema(df)
    print("=" * 50)
    profile_nulls(df)
    print("=" * 50)
    profile_uniqueness(df, "order_id")
    print("=" * 50)
    profile_distribution(df)
    print("=" * 50)
    profile_temporal(df, "2024-01-15")
    print("=" * 50)
    profile_row_count(df)
    print("=" * 50)
    profile_partition_sizes(df)
    print("=" * 50)
    profile_growth_rate(df)


run_full_profile(df)
