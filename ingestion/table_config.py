TABLE_CONFIG = [
    {
        "name": "customers",
        "file": "customers.csv",
        "table": "raw_customers",
        "pk": "customer_id",
        "required_columns": [
            "customer_id",
            "customer_unique_id",
            "customer_city",
            "customer_state"
        ]
    },
    {
        "name": "orders",
        "file": "orders.csv",
        "table": "raw_orders",
        "pk": "order_id",
        "required_columns": [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp"
        ]
    }
]