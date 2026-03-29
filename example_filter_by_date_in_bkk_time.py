import pytz

import pandas as pd


def filter_by_date_bkk(df: pd.DataFrame, partition_date: str) -> pd.DataFrame:
    """
    Filter orders by event date in Asia/Bangkok time (UTC+7).
    Source data is assumed to be UTC — convert explicitly before comparing.
    """
    BKK = pytz.timezone("Asia/Bangkok")

    # Make event_time timezone-aware (UTC), then convert to Bangkok
    event_time_bkk = df["event_time"].dt.tz_localize("UTC").dt.tz_convert(BKK)
    target_date = pd.Timestamp(partition_date).date()

    return df[event_time_bkk.dt.date == target_date].copy()


# Demonstrate the difference
sample_utc = pd.DataFrame({
    "order_id": ["A", "B"],
    "event_time": pd.to_datetime(["2024-01-15 18:00:00", "2024-01-15 23:00:00"]),  # UTC
    "amount": [100, 200]
})

# In Bangkok (UTC+7): 18:00 UTC = 01:00 Jan 16 BKK → belongs to Jan 16, NOT Jan 15
result = filter_by_date_bkk(sample_utc, "2024-01-16")
print("Orders on Jan 16 (Bangkok time):")
print(result)
