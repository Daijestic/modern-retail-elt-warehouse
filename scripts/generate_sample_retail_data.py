"""
Generate synthetic retail CSV files for Modern Retail ELT Warehouse.

Run:
    python scripts/generate_sample_retail_data.py
    python scripts/project_cli.py prepare-sample-data

Output:
    data/raw/customers.csv
    data/raw/orders.csv
    data/raw/order_items.csv
    data/raw/products.csv
    data/raw/payments.csv
    data/raw/shipments.csv
"""
from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

OUT_DIR = Path("data/raw")
OUT_DIR.mkdir(parents=True, exist_ok=True)

cities_states = [
    ("ha_noi", "HN", "10000"),
    ("ho_chi_minh", "HCM", "70000"),
    ("da_nang", "DN", "55000"),
    ("hai_phong", "HP", "18000"),
    ("can_tho", "CT", "90000"),
    ("binh_duong", "BD", "75000"),
    ("dong_nai", "DNA", "76000"),
    ("bac_ninh", "BN", "16000"),
    ("quang_ninh", "QN", "20000"),
    ("nghe_an", "NA", "43000"),
]

categories = [
    "electronics",
    "home_appliances",
    "furniture",
    "beauty_health",
    "sports_leisure",
    "books",
    "toys",
    "fashion",
    "auto_accessories",
    "grocery",
]

customers = []
for customer_index in range(1, 31):
    city, state, zip_prefix = random.choice(cities_states)
    customers.append(
        {
            "customer_id": f"cus_{customer_index:04d}",
            "customer_unique_id": f"uniq_{random.randint(1, 22):04d}",
            "customer_zip_code_prefix": zip_prefix,
            "customer_city": city,
            "customer_state": state,
        }
    )

products = []
for product_index in range(1, 26):
    products.append(
        {
            "product_id": f"prd_{product_index:04d}",
            "product_category_name": random.choice(categories),
        }
    )

orders = []
start_time = datetime(2025, 1, 1, 8, 0, 0)
statuses = ["delivered"] * 48 + ["shipped"] * 5 + ["invoiced"] * 4 + ["canceled"] * 3

for order_index in range(1, 61):
    customer = random.choice(customers)
    purchase_time = start_time + timedelta(
        days=random.randint(0, 150),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )
    orders.append(
        {
            "order_id": f"ord_{order_index:05d}",
            "customer_id": customer["customer_id"],
            "order_status": statuses[order_index - 1],
            "order_purchase_timestamp": purchase_time.strftime("%Y-%m-%d %H:%M:%S"),
        }
    )

order_items = []
for order in orders:
    item_count = random.choices([1, 2, 3], weights=[0.65, 0.25, 0.10])[0]
    selected_products = random.sample(products, item_count)
    for item_index, product in enumerate(selected_products, start=1):
        order_items.append(
            {
                "order_id": order["order_id"],
                "order_item_id": item_index,
                "product_id": product["product_id"],
                "seller_id": f"sel_{random.randint(1, 10):03d}",
                "price": int(round(random.uniform(50000, 2500000), -3)),
                "freight_value": int(round(random.uniform(12000, 120000), -3)),
            }
        )

payments = []
for order in orders:
    total_value = sum(
        item["price"] + item["freight_value"]
        for item in order_items
        if item["order_id"] == order["order_id"]
    )
    if random.random() < 0.15:
        first_payment = int(total_value * random.uniform(0.35, 0.65))
        payments.append(
            {
                "order_id": order["order_id"],
                "payment_sequential": 1,
                "payment_type": random.choice(["credit_card", "bank_transfer", "e_wallet"]),
                "payment_value": first_payment,
            }
        )
        payments.append(
            {
                "order_id": order["order_id"],
                "payment_sequential": 2,
                "payment_type": random.choice(["voucher", "e_wallet"]),
                "payment_value": total_value - first_payment,
            }
        )
    else:
        payments.append(
            {
                "order_id": order["order_id"],
                "payment_sequential": 1,
                "payment_type": random.choice(
                    ["credit_card", "bank_transfer", "e_wallet", "cash_on_delivery"]
                ),
                "payment_value": total_value,
            }
        )

shipments = []
for order in orders:
    purchase_datetime = datetime.strptime(order["order_purchase_timestamp"], "%Y-%m-%d %H:%M:%S")
    estimated_delivery = purchase_datetime + timedelta(days=random.randint(3, 9))
    delivered_datetime = estimated_delivery + timedelta(
        days=random.choices(
            [-2, -1, 0, 1, 2, 3, 5],
            weights=[0.12, 0.18, 0.30, 0.18, 0.12, 0.07, 0.03],
        )[0]
    )
    if delivered_datetime <= purchase_datetime:
        delivered_datetime = purchase_datetime + timedelta(days=1)
    shipments.append(
        {
            "order_id": order["order_id"],
            "delivered_customer_date": delivered_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            "estimated_delivery_date": estimated_delivery.strftime("%Y-%m-%d %H:%M:%S"),
        }
    )

files_data = {
    "customers.csv": customers,
    "orders.csv": orders,
    "order_items.csv": order_items,
    "products.csv": products,
    "payments.csv": payments,
    "shipments.csv": shipments,
}

for filename, rows in files_data.items():
    with (OUT_DIR / filename).open("w", newline="", encoding="utf-8") as file_handle:
        writer = csv.DictWriter(file_handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

print(f"Generated {len(files_data)} CSV files in {OUT_DIR}")
