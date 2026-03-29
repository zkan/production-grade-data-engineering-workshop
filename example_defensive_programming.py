import pandas as pd


def safe_compute_revenue(df: pd.DataFrame) -> float:
    """
    Compute total revenue with defensive checks.
    Raises clear errors instead of returning silent wrong values.
    """
    # Guard: column must exist
    if "amount" not in df.columns:
        raise ValueError("Column 'amount' is missing — cannot compute revenue.")

    # Guard: must have rows
    if df.empty:
        raise ValueError("DataFrame is empty — no data to aggregate.")

    # Guard: warn on unexpected nulls
    null_count = df["amount"].isnull().sum()
    if null_count > 0:
        print(f"WARNING: {null_count} null amount(s) will be excluded from revenue sum.")

    # Guard: warn on negatives
    neg_count = (df["amount"] < 0).sum()
    if neg_count > 0:
        print(f"WARNING: {neg_count} negative amount(s) found — verify refund handling.")

    total = df["amount"].sum(skipna=True)

    # Guard: result sanity check
    if total < 0:
        raise ValueError(f"Revenue total is negative ({total}) — check your data or filters.")

    return total


# Test all the guards
df_good    = pd.DataFrame([{"amount": 100}, {"amount": 200}])
df_nulls   = pd.DataFrame([{"amount": 100}, {"amount": None}])
df_neg     = pd.DataFrame([{"amount": 100}, {"amount": -50}])
df_missing = pd.DataFrame([{"order_id": "A"}])  # no 'amount' column
df_empty   = pd.DataFrame(columns=["amount"])

for label, data in [("Good data", df_good), ("Nulls", df_nulls), ("Negatives", df_neg)]:
    print(f"\n--- {label} ---")
    try:
        result = safe_compute_revenue(data)
        print(f"Revenue: {result}")
    except ValueError as e:
        print(f"Error: {e}")

for label, data in [("Missing column", df_missing), ("Empty DataFrame", df_empty)]:
    print(f"\n--- {label} ---")
    try:
        safe_compute_revenue(data)
    except ValueError as e:
        print(f"Caught expected error: {e}")
