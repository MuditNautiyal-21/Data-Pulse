"""
Creating fake e-commerce data for testing Data-Pulse.
Run this ONCE: python sample_data/generate_data.py
"""

import pandas as pd
import random
from datetime import datetime, timedelta

random.seed(42)

# -- Data for 1000 orders --
orders = []
for i in range(1, 1001):
    order_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))
    order = {
        "order_id": i,
        "customer_id": random.randint(1, 200),
        "order_date": order_date.strftime("%Y-%m-%d"),
        "amount": round(random.uniform(10, 500), 2),
        "status": random.choice(["completed", "pending", "cancelled", "shipped"]),
    }

    # Making 5% of amounts null or missing
    if random.random() < 0.05:
        order["amount"] = None
    # Making 2% of amounts negative (we can call it bad data!)
    if random.random() < 0.02:
        order["amount"] = -round(random.uniform(1, 100), 2)
    # Making 3% of dates in the future (to give a suspicious look)
    if random.random() < 0.03:
        order["order_date"] = "2026-06-15"

    orders.append(order)

# Adding 5 duplicate order_ids (since its a common bug in real-world)
for i in range(5):
    orders.append(orders[random.randint(0, 999)].copy())

pd.DataFrame(orders).to_csv("sample_data/orders.csv", index=False)
print(f"Created orders.csv: {len(orders)} rows")

# -- About 200 customers --
customers = []
for i in range(1, 201):
    customer = {
        "customer_id": i,
        "name": f"Customer_{i}",
        "email": f"customer_{i}@example.com",
        "signup_date": (
            datetime(2023, 1, 1) + timedelta(days=random.randint(0, 730))
        ).strftime("%Y-%m-%d"),
        "country": random.choice(["US", "UK", "DE", "FR", "IN", "CA"]),
    }
    # Make 8% of emails null
    if random.random() < 0.08:
        customer["email"] = None
    customers.append(customer)

pd.DataFrame(customers).to_csv("sample_data/customers.csv", index=False)
print(f"Created customers.csv: {len(customers)} rows")
print("\nDone! Check your sample_data/ directory")