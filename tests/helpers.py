from __future__ import annotations

import csv
import subprocess
from pathlib import Path

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def build_valid_dataset() -> dict[str, list[dict[str, str]]]:
    return {
        "customers.csv": [
            {
                "customer_id": "cust_001",
                "customer_unique_id": "uniq_001",
                "customer_zip_code_prefix": "10000",
                "customer_city": "ha_noi",
                "customer_state": "HN",
            },
            {
                "customer_id": "cust_002",
                "customer_unique_id": "uniq_002",
                "customer_zip_code_prefix": "70000",
                "customer_city": "ho_chi_minh",
                "customer_state": "HCM",
            },
        ],
        "orders.csv": [
            {
                "order_id": "ord_001",
                "customer_id": "cust_001",
                "order_status": "delivered",
                "order_purchase_timestamp": "2025-01-01 10:00:00",
                "order_approved_at": "2025-01-01 10:15:00",
                "order_delivered_customer_date": "2025-01-03 12:00:00",
                "order_estimated_delivery_date": "2025-01-04 12:00:00",
            },
            {
                "order_id": "ord_002",
                "customer_id": "cust_002",
                "order_status": "shipped",
                "order_purchase_timestamp": "2025-01-02 09:30:00",
                "order_approved_at": "2025-01-02 10:00:00",
                "order_delivered_customer_date": "",
                "order_estimated_delivery_date": "2025-01-07 09:30:00",
            },
        ],
        "order_items.csv": [
            {
                "order_id": "ord_001",
                "order_item_id": "1",
                "product_id": "prd_001",
                "seller_id": "sel_001",
                "shipping_limit_date": "2025-01-02 12:00:00",
                "price": "100.00",
                "freight_value": "10.00",
            },
            {
                "order_id": "ord_001",
                "order_item_id": "2",
                "product_id": "prd_002",
                "seller_id": "sel_001",
                "shipping_limit_date": "2025-01-02 12:00:00",
                "price": "50.00",
                "freight_value": "5.00",
            },
            {
                "order_id": "ord_002",
                "order_item_id": "1",
                "product_id": "prd_002",
                "seller_id": "sel_002",
                "shipping_limit_date": "2025-01-03 10:00:00",
                "price": "30.00",
                "freight_value": "7.00",
            },
        ],
        "products.csv": [
            {
                "product_id": "prd_001",
                "product_category_name": "electronics",
                "product_name_lenght": "10",
                "product_description_lenght": "100",
                "product_photos_qty": "2",
                "product_weight_g": "500",
                "product_length_cm": "15",
                "product_height_cm": "5",
                "product_width_cm": "7",
            },
            {
                "product_id": "prd_002",
                "product_category_name": "books",
                "product_name_lenght": "8",
                "product_description_lenght": "60",
                "product_photos_qty": "1",
                "product_weight_g": "250",
                "product_length_cm": "21",
                "product_height_cm": "2",
                "product_width_cm": "14",
            },
        ],
        "payments.csv": [
            {
                "order_id": "ord_001",
                "payment_sequential": "1",
                "payment_type": "credit_card",
                "payment_installments": "1",
                "payment_value": "165.00",
            },
            {
                "order_id": "ord_002",
                "payment_sequential": "1",
                "payment_type": "voucher",
                "payment_installments": "1",
                "payment_value": "37.00",
            },
        ],
        "shipments.csv": [
            {
                "order_id": "ord_001",
                "delivered_customer_date": "2025-01-03 12:00:00",
                "estimated_delivery_date": "2025-01-04 12:00:00",
            },
            {
                "order_id": "ord_002",
                "delivered_customer_date": "",
                "estimated_delivery_date": "2025-01-07 09:30:00",
            },
        ],
    }


def write_dataset(destination: Path, dataset: dict[str, list[dict[str, str]]]) -> Path:
    destination.mkdir(parents=True, exist_ok=True)
    for filename, rows in dataset.items():
        if not rows:
            raise ValueError(f"Dataset rows cannot be empty for {filename}")
        with (destination / filename).open("w", newline="", encoding="utf-8") as file_handle:
            writer = csv.DictWriter(file_handle, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    return destination


def query_scalar(engine, sql: str):
    with engine.connect() as connection:
        return connection.execute(text(sql)).scalar_one()


def run_command(command: list[str], env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
