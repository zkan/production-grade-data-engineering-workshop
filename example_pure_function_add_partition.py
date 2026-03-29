import pandas as pd


def add_partition_date(df: pd.DataFrame, partition_date: str) -> pd.DataFrame:
    """
    Pure function: returns a new DataFrame, does not modify input.
    Same inputs → always same output.
    No I/O, no global state, no side effects.
    """
    return df.assign(partition_date=partition_date)


# Easy to test:
sample = pd.DataFrame([{"order_id": "A", "amount": 100}])
result = add_partition_date(sample, "2024-01-15")
assert result["partition_date"].iloc[0] == "2024-01-15"
assert "partition_date" not in sample.columns  # original not mutated!
print("✅ Pure function test passed")
