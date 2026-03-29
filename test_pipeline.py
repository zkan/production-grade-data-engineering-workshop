import pandas as pd


def calculate_revenue(df: pd.DataFrame) -> float:
    """Pure function — easy to test."""
    return df[df["status"] == "completed"]["amount"].sum()


# --- TESTS ---
def test_revenue_only_counts_completed():
    data = pd.DataFrame([
        {"order_id": "A", "amount": 100, "status": "completed"},
        {"order_id": "B", "amount": 200, "status": "cancelled"},  # should NOT count
        {"order_id": "C", "amount":  50, "status": "refund"},      # should NOT count
    ])
    assert calculate_revenue(data) == 100


def test_revenue_with_no_completed_orders():
    data = pd.DataFrame([
        {"order_id": "A", "amount": 100, "status": "cancelled"},
    ])
    assert calculate_revenue(data) == 0


def test_revenue_sums_multiple_completed():
    data = pd.DataFrame([
        {"order_id": "A", "amount": 100, "status": "completed"},
        {"order_id": "B", "amount": 200, "status": "completed"},
    ])
    assert calculate_revenue(data) == 300


# Run all tests
import traceback


for test_fn in [test_revenue_only_counts_completed,
                test_revenue_with_no_completed_orders,
                test_revenue_sums_multiple_completed]:
    try:
        test_fn()
        print(f"✅ PASS: {test_fn.__name__}")
    except AssertionError as e:
        print(f"❌ FAIL: {test_fn.__name__}")
        traceback.print_exc()


from data_modeling import clean_layer


def assert_schema(df: pd.DataFrame, required_columns: list):
    """Fail if any required column is missing."""
    missing = [c for c in required_columns if c not in df.columns]
    assert not missing, f"Missing columns: {missing}"
    print("✅ Schema test passed")


# Test it
assert_schema(
    clean_layer,
    required_columns=["order_id", "customer_id", "country",
                      "amount", "status", "partition_date"]
)

# Try failing it
try:
    assert_schema(clean_layer, required_columns=["order_id", "THIS_COLUMN_DOESNT_EXIST"])
except AssertionError as e:
    print(f"❌ Schema test caught error: {e}")


from data_modeling import mart


def assert_no_duplicates(df: pd.DataFrame, primary_key: list):
    """Fail if primary key is not unique."""
    dupes = df[df.duplicated(subset=primary_key, keep=False)]
    assert dupes.empty, f"Duplicate primary key found:\n{dupes}"
    print(f"✅ No duplicates in {primary_key}")


# Should pass on clean layer
assert_no_duplicates(clean_layer, primary_key=["order_id"])


# Should pass on mart (one row per date + country)
assert_no_duplicates(mart, primary_key=["partition_date", "country"])


def assert_no_negative_revenue(df: pd.DataFrame, column: str):
    """Revenue should never be negative in the mart."""
    negatives = df[df[column] < 0]
    assert negatives.empty, f"Negative values in '{column}':\n{negatives}"
    print(f"✅ No negative values in '{column}'")


def assert_no_nulls(df: pd.DataFrame, columns: list):
    """Specified columns should have no nulls."""
    for col in columns:
        nulls = df[col].isnull().sum()
        assert nulls == 0, f"Column '{col}' has {nulls} null value(s)"
    print(f"✅ No nulls in: {columns}")


assert_no_negative_revenue(mart, "total_revenue")
assert_no_nulls(mart, ["partition_date", "country", "total_revenue"])


def assert_revenue_reconciliation(raw_df, mart_df, partition_date: str, tolerance: float = 0.01):
    """
    Cross-check: mart total revenue must equal the sum computed directly from source.
    This catches bugs in aggregation logic.
    """
    # Ground truth from source
    expected = (
        raw_df[
            (raw_df["status"] == "completed") &
            (raw_df["event_time"].dt.date.astype(str) == partition_date)
        ]["amount"].sum()
    )
    
    # What the mart says
    actual = mart_df[mart_df["partition_date"] == partition_date]["total_revenue"].sum()
    
    diff = abs(expected - actual)
    assert diff <= tolerance, (
        f"Revenue mismatch for {partition_date}! "
        f"Expected: {expected}, Got: {actual}, Diff: {diff}"
    )
    print(f"✅ Revenue reconciliation passed for {partition_date} "
          f"(expected={expected}, actual={actual})")


assert_revenue_reconciliation(clean_layer, mart, "2024-01-15")


def assert_data_freshness(df: pd.DataFrame, date_column: str, max_age_days: int = 1):
    """
    Fail if the most recent partition is older than expected.
    Catches pipelines that silently stopped running.
    """
    latest = pd.to_datetime(df[date_column]).max()
    age = (pd.Timestamp.now() - latest).days
    assert age <= max_age_days, (
        f"Data is stale! Latest partition: {latest.date()}, Age: {age} days"
    )
    print(f"✅ Freshness check passed (latest: {latest.date()}, age: {age} day(s))")


# This will likely warn since our sample data is from 2024
try:
    assert_data_freshness(mart, "partition_date", max_age_days=1)
except AssertionError as e:
    print(f"⚠️  {e}")


def run_data_quality_suite(raw_df, clean_df, mart_df, partition_date: str):
    print(f"\n{'='*50}")
    print(f"Data Quality Suite — {partition_date}")
    print('='*50)
    
    checks = [
        ("Schema check",             lambda: assert_schema(clean_df, ["order_id", "customer_id", "country", "amount", "status", "partition_date"])),
        ("No duplicates (clean)",    lambda: assert_no_duplicates(clean_df, ["order_id"])),
        ("No duplicates (mart)",     lambda: assert_no_duplicates(mart_df, ["partition_date", "country"])),
        ("No negative revenue",      lambda: assert_no_negative_revenue(mart_df, "total_revenue")),
        ("No nulls in mart",         lambda: assert_no_nulls(mart_df, ["partition_date", "country", "total_revenue"])),
        ("Revenue reconciliation",   lambda: assert_revenue_reconciliation(raw_df, mart_df, partition_date)),
    ]
    
    passed, failed = 0, 0
    for name, check in checks:
        try:
            check()
            passed += 1
        except (AssertionError, Exception) as e:
            print(f"❌ FAIL [{name}]: {e}")
            failed += 1
    
    print(f"\nResult: {passed} passed, {failed} failed")
    if failed > 0:
        raise RuntimeError("Data quality suite failed — pipeline output should NOT be published.")


run_data_quality_suite(clean_layer, clean_layer, mart, "2024-01-15")
