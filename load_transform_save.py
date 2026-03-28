import pandas as pd


# Load data
df = pd.read_csv("orders.csv")

# Transform
df2 = df[df["status"] == "completed"]
df3 = df2.copy()
result = df3.groupby("country")["amount"].sum().reset_index()
result.columns = ["country", "revenue"]
result["run_date"] = "2024-01-15"  # hardcoded date!

# Save
result.to_csv("revenue_2024-01-15.csv", index=False)
print("done")
