import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

np.random.seed(42)
random.seed(42)

# -------------------------
# CONFIG
# -------------------------
NUM_SALES = 10000
NUM_PRODUCTS = 50
NUM_STORES = 100
NUM_CUSTOMERS = 2000

countries = [
    "USA", "Canada", "UK", "Germany", "France",
    "Spain", "Italy", "Netherlands", "Sweden", "Australia"
]

languages = ["English", "French", "German", "Spanish", "Italian"]

furniture_categories = [
    "Chair", "Table", "Sofa", "Bed", "Cabinet",
    "Desk", "Shelf", "Wardrobe", "Stool", "Bench"
]

payment_methods = ["Credit Card", "Debit Card", "PayPal"]
device_types = ["Mobile", "Desktop"]

# -------------------------
# DATE DIMENSION
# -------------------------
start_date = datetime(2025, 1, 1)
dates = [start_date + timedelta(days=i) for i in range(365)]

dim_date = pd.DataFrame({
    "date_id": [int(d.strftime("%Y%m%d")) for d in dates],
    "full_date": dates,
    "day": [d.day for d in dates],
    "month": [d.month for d in dates],
    "year": [d.year for d in dates],
    "quarter": [(d.month - 1)//3 + 1 for d in dates],
    "day_of_week": [d.weekday() for d in dates],
    "is_weekend": [d.weekday() >= 5 for d in dates]
})

# -------------------------
# PRODUCT DIMENSION
# -------------------------
products = []
for i in range(NUM_PRODUCTS):
    cat = random.choice(furniture_categories)
    products.append({
        "product_id": i + 1,
        "product_name": f"{cat} {i+1}",
        "category": "Furniture",
        "subcategory": cat,
        "brand": f"Brand_{random.randint(1,10)}",
        "unit_cost": round(random.uniform(20, 300), 2)
    })

dim_product = pd.DataFrame(products)

# -------------------------
# STORE DIMENSION
# -------------------------
stores = []
for i in range(NUM_STORES):
    country = random.choice(countries)
    stores.append({
        "store_id": i + 1,
        "store_name": f"Store_{i+1}",
        "city": f"City_{random.randint(1,50)}",
        "state": f"State_{random.randint(1,20)}",
        "country": country
    })

dim_store = pd.DataFrame(stores)

# -------------------------
# CUSTOMER DIMENSION
# -------------------------
customers = []
for i in range(NUM_CUSTOMERS):
    customers.append({
        "customer_id": i + 1,
        "country": random.choice(countries),
        "language": random.choice(languages)
    })

dim_customer = pd.DataFrame(customers)

# -------------------------
# FACT SALES
# -------------------------
sales = []
today = int(datetime.today().strftime("%Y%m%d"))

for i in range(NUM_SALES):
    date = random.choice(dim_date["date_id"].tolist())

    # ensure some today's data
    if i < 200:
        date = today

    quantity = np.random.randint(1, 5)
    unit_price = round(random.uniform(50, 1000), 2)
    discount = round(random.uniform(0, 0.3), 2)
    total = round(quantity * unit_price * (1 - discount), 2)

    sales.append({
        "sale_id": i + 1,
        "order_id": random.randint(1, 4000),
        "date_id": date,
        "product_id": random.randint(1, NUM_PRODUCTS),
        "customer_id": random.randint(1, NUM_CUSTOMERS),
        "store_id": random.randint(1, NUM_STORES),
        "quantity": quantity,
        "unit_price": unit_price,
        "discount": discount,
        "total_amount": total,
        "payment_method": random.choice(payment_methods),
        "device_type": random.choice(device_types)
    })

fact_sales = pd.DataFrame(sales)

# -------------------------
# SAVE FILES
# -------------------------
dim_date.to_csv("data/dim_date.csv", index=False)
dim_product.to_csv("data/dim_product.csv", index=False)
dim_store.to_csv("data/dim_store.csv", index=False)
dim_customer.to_csv("data/dim_customer.csv", index=False)
fact_sales.to_csv("data/fact_sales.csv", index=False)

print("Dataset generated successfully!")
