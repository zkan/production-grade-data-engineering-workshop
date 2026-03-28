import pandas as pd


# Simulate raw order data from an upstream system
raw_data = [
    # (order_id, customer_id, country, amount, status, event_time)
    ("ORD-001", "C-01", "TH", 250.0,  "completed",  "2024-01-15 10:00:00"),
    ("ORD-002", "C-02", "TH", 180.0,  "completed",  "2024-01-15 11:00:00"),
    ("ORD-003", "C-03", None,  90.0,  "completed",  "2024-01-15 12:00:00"),  # null country
    ("ORD-004", "C-01", "SG", 320.0,  "cancelled",  "2024-01-15 13:00:00"),
    ("ORD-002", "C-02", "TH", 180.0,  "completed",  "2024-01-15 11:00:00"),  # DUPLICATE
    ("ORD-005", "C-04", "TH", -50.0,  "refund",     "2024-01-15 14:00:00"),  # negative amount
    ("ORD-006", "C-05", "MY", 410.0,  "completed",  "2024-01-12 09:00:00"),  # late arrival (3 days late)
    ("ORD-007", "C-06", "TH",  None,  "completed",  "2024-01-15 15:00:00"),  # null amount
    ("ORD-008", "C-07", "TH", 150.0,  "pending",    "2024-01-15 16:00:00"),
    ("ORD-009", "C-01", "TH", 200.0,  "completed",  "2024-01-15 17:00:00"),
]

df = pd.DataFrame(raw_data, columns=[
    "order_id", "customer_id", "country",
    "amount", "status", "event_time"
])
df["event_time"] = pd.to_datetime(df["event_time"])
df["amount"] = pd.to_numeric(df["amount"])

print(df.to_string())
df.to_csv("orders.csv", index=0)
