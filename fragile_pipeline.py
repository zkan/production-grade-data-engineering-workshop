import pandas as pd


def fragile_pipeline(df):
    """
    BAD: Blind append. Running this twice doubles the revenue.
    """
    revenue = df.groupby("status")["amount"].sum()
    return revenue


# Load data
df = pd.read_csv("orders.csv")

# Simulate running twice (accident or retry)
result1 = fragile_pipeline(df)
result2 = fragile_pipeline(df)
total = result1 + result2  # WRONG — doubled!

print("Doubled revenue (bug):")
print(total)
