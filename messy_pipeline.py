import pandas as pd


# Load data
df = pd.read_csv("orders.csv")

# Clean and compute
df2 = df.drop_duplicates()
df3 = df2[df2["status"] == "completed"]
df4 = df3.copy()
df4["amount"] = pd.to_numeric(df4["amount"], errors="coerce")
df4["country"] = df4["country"].fillna("UNKNOWN")
result = df4.groupby("country")["amount"].sum().reset_index()
result.columns = ["country", "revenue"]
result["run_date"] = "2024-01-15"  # hardcoded date!

# Save
result.to_csv("revenue_2024-01-15.csv", index=False)
print("done")
