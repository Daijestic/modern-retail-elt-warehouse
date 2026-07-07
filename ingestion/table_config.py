from __future__ import annotations

from dataclasses import dataclass, field

LANDING_METADATA_COLUMNS = (
    "_ingestion_run_id",
    "_source_file",
    "_source_row_number",
    "_ingested_at",
    "_file_checksum",
    "_source_modified_at",
)


@dataclass(frozen=True)
class TableConfig:
    name: str
    file_name: str
    target_table: str
    primary_key: tuple[str, ...]
    required_columns: tuple[str, ...]
    optional_columns: tuple[str, ...] = ()
    numeric_columns: tuple[str, ...] = ()
    timestamp_columns: tuple[str, ...] = ()
    accepted_values: dict[str, tuple[str, ...]] = field(default_factory=dict)
    non_negative_columns: tuple[str, ...] = ()
    enabled: bool = True

    @property
    def all_source_columns(self) -> tuple[str, ...]:
        return self.required_columns + tuple(
            column for column in self.optional_columns if column not in self.required_columns
        )

    @property
    def known_columns(self) -> set[str]:
        return set(self.all_source_columns)

    @property
    def target_columns(self) -> tuple[str, ...]:
        return self.all_source_columns + LANDING_METADATA_COLUMNS


TABLE_CONFIG = [
    TableConfig(
        name="customers",
        file_name="customers.csv",
        target_table="raw_customers",
        primary_key=("customer_id",),
        required_columns=(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ),
    ),
    TableConfig(
        name="orders",
        file_name="orders.csv",
        target_table="raw_orders",
        primary_key=("order_id",),
        required_columns=(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
        ),
        optional_columns=(
            "order_approved_at",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ),
        timestamp_columns=(
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ),
        accepted_values={
            "order_status": (
                "approved",
                "canceled",
                "created",
                "delivered",
                "invoiced",
                "processing",
                "shipped",
                "unavailable",
            )
        },
    ),
    TableConfig(
        name="order_items",
        file_name="order_items.csv",
        target_table="raw_order_items",
        primary_key=("order_id", "order_item_id"),
        required_columns=(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "price",
            "freight_value",
        ),
        optional_columns=("shipping_limit_date",),
        numeric_columns=("order_item_id", "price", "freight_value"),
        timestamp_columns=("shipping_limit_date",),
        non_negative_columns=("price", "freight_value"),
    ),
    TableConfig(
        name="products",
        file_name="products.csv",
        target_table="raw_products",
        primary_key=("product_id",),
        required_columns=(
            "product_id",
            "product_category_name",
        ),
        optional_columns=(
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ),
        numeric_columns=(
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ),
    ),
    TableConfig(
        name="payments",
        file_name="payments.csv",
        target_table="raw_payments",
        primary_key=("order_id", "payment_sequential"),
        required_columns=(
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_value",
        ),
        optional_columns=("payment_installments",),
        numeric_columns=("payment_sequential", "payment_installments", "payment_value"),
        non_negative_columns=("payment_value",),
        accepted_values={
            "payment_type": (
                "bank_transfer",
                "cash_on_delivery",
                "credit_card",
                "debit_card",
                "e_wallet",
                "voucher",
            )
        },
    ),
    TableConfig(
        name="shipments",
        file_name="shipments.csv",
        target_table="raw_shipments",
        primary_key=("order_id",),
        required_columns=(
            "order_id",
            "delivered_customer_date",
            "estimated_delivery_date",
        ),
        timestamp_columns=(
            "delivered_customer_date",
            "estimated_delivery_date",
        ),
    ),
]


def get_enabled_table_configs() -> list[TableConfig]:
    return [table_config for table_config in TABLE_CONFIG if table_config.enabled]


def get_table_config(source_name: str) -> TableConfig:
    for table_config in TABLE_CONFIG:
        if table_config.name == source_name:
            return table_config
    raise KeyError(f"Unknown source configuration: {source_name}")
