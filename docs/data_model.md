# Data Model

## Overview

This project models retail data into staging models, core marts, and analytics marts.

The current warehouse follows a simple dimensional modeling approach:

```text
staging models
        ↓
dimension tables
        ↓
fact tables
        ↓
analytics marts
```

## Staging Models

| Model | Grain | Description |
|---|---|---|
| stg_customers | One row per customer_id | Cleaned customer records |
| stg_orders | One row per order_id | Cleaned order records |
| stg_order_items | One row per order item | Cleaned order item records |
| stg_products | One row per product_id | Cleaned product records |
| stg_payments | One row per payment record | Cleaned payment records |
| stg_shipments | One row per order_id | Cleaned shipment records |

## Core Marts

### dim_customers

Grain:

```text
One row per customer_id
```

Purpose:

Stores customer attributes such as customer unique ID, city, state, and zip code prefix.

Primary key:

```text
customer_id
```

---

### dim_products

Grain:

```text
One row per product_id
```

Purpose:

Stores product attributes such as product category, weight, length, height, and width.

Primary key:

```text
product_id
```

Note:

The current dataset does not contain a product name field, so product analysis uses `product_id` and `product_category_name`.

---

### fact_orders

Grain:

```text
One row per order_id
```

Purpose:

Stores order-level metrics and delivery information.

Key metrics:

- item_count
- total_item_price
- total_freight_value
- total_order_value
- total_payment_value
- delivery_days
- is_late_delivery

Important modeling decision:

Order item metrics and payment metrics are aggregated by `order_id` before joining into `fact_orders`.

This prevents revenue double counting when one order has multiple items or multiple payment records.

---

### fact_order_items

Grain:

```text
One row per order item
```

Purpose:

Stores item-level order metrics.

Key metrics:

- item_price
- freight_value
- gross_revenue

Business logic:

```text
gross_revenue = item_price + freight_value
```

## Analytics Marts

### mart_daily_revenue

Grain:

```text
One row per order_date
```

Metrics:

- total_orders
- total_revenue
- average_order_value

Purpose:

Used for revenue trend analysis.

---

### mart_product_performance

Grain:

```text
One row per product_id
```

Metrics:

- total_quantity
- total_orders
- total_item_revenue
- total_freight_value
- total_revenue

Purpose:

Used to analyze product and product category performance.

---

### mart_delivery_performance

Grain:

```text
One row per order_id
```

Metrics:

- delivery_days
- is_late_delivery
- late_delivery_flag

Purpose:

Used to analyze delivery delays and shipping performance.

## Future Data Model Improvements

Planned improvements:

- add `dim_dates`
- add `dim_sellers` if seller data is added
- add `mart_customer_retention`
- add custom tests for revenue consistency
- add SCD Type 2 snapshot for product dimension
- add incremental model for daily revenue
