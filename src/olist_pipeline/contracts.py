"""Data contracts for the six landing feeds.

Each feed has an explicit schema. Bronze parses against it and never infers types, so a
producer that adds or renames a field cannot silently change our tables; unknown fields are
kept in `_rescued_data` instead (ADR-0006).
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

MONEY = DecimalType(12, 2)


@dataclass(frozen=True)
class Feed:
    name: str
    fmt: str  # "json" (JSON lines) or "parquet"
    kind: str  # "append", "cdc" or "snapshot"
    schema: StructType
    keys: tuple[str, ...]


ORDERS_CDC = Feed(
    name="orders_cdc",
    fmt="json",
    kind="cdc",
    keys=("order_id",),
    schema=StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("order_status", StringType()),
        StructField("order_purchase_ts", TimestampType()),
        StructField("order_approved_ts", TimestampType()),
        StructField("order_delivered_carrier_ts", TimestampType()),
        StructField("order_delivered_customer_ts", TimestampType()),
        StructField("order_estimated_delivery_ts", TimestampType()),
        StructField("change_seq", IntegerType()),
        StructField("change_ts", TimestampType()),
    ]),
)

ORDER_ITEMS = Feed(
    name="order_items",
    fmt="json",
    kind="append",
    keys=("order_id", "order_item_id"),
    schema=StructType([
        StructField("order_id", StringType()),
        StructField("order_item_id", IntegerType()),
        StructField("product_id", StringType()),
        StructField("seller_id", StringType()),
        StructField("order_purchase_ts", TimestampType()),
        StructField("shipping_limit_ts", TimestampType()),
        StructField("price", MONEY),
        StructField("freight_value", MONEY),
    ]),
)

CUSTOMERS = Feed(
    name="customers",
    fmt="json",
    kind="append",
    keys=("customer_id",),
    schema=StructType([
        StructField("customer_id", StringType()),
        StructField("customer_unique_id", StringType()),
        StructField("customer_zip_code_prefix", StringType()),
        StructField("customer_city", StringType()),
        StructField("customer_state", StringType()),
    ]),
)

REVIEWS = Feed(
    name="reviews",
    fmt="json",
    kind="append",
    # review_id alone is NOT unique in Olist: one review can cover several orders.
    keys=("review_id", "order_id"),
    schema=StructType([
        StructField("review_id", StringType()),
        StructField("order_id", StringType()),
        StructField("review_score", IntegerType()),
        StructField("review_creation_ts", TimestampType()),
        StructField("review_answer_ts", TimestampType()),
    ]),
)

SELLERS = Feed(
    name="sellers",
    fmt="parquet",
    kind="snapshot",
    keys=("seller_id",),
    schema=StructType([
        StructField("seller_id", StringType()),
        StructField("seller_zip_code_prefix", StringType()),
        StructField("seller_city", StringType()),
        StructField("seller_state", StringType()),
    ]),
)

PRODUCTS = Feed(
    name="products",
    fmt="parquet",
    kind="snapshot",
    keys=("product_id",),
    schema=StructType([
        StructField("product_id", StringType()),
        StructField("product_category_name", StringType()),
        StructField("product_category_name_english", StringType()),
        StructField("product_weight_g", IntegerType()),
        StructField("product_photos_qty", IntegerType()),
    ]),
)

FEEDS: dict[str, Feed] = {f.name: f for f in (ORDERS_CDC, ORDER_ITEMS, CUSTOMERS, REVIEWS, SELLERS, PRODUCTS)}
