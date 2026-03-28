# Deterministic transformation
def transform(data):
    return [x * 2 for x in data]

# Non-idempotent load
def load(storage, transformed_data):
    for row in transformed_data:
        storage.append(row)

# Simulate pipeline
storage = []

data = [1, 2, 3]

# Run 1
result = transform(data)
load(storage, result)
print("After run 1:", storage)

# Run 2 (same input)
result = transform(data)
load(storage, result)
print("After run 2:", storage)
