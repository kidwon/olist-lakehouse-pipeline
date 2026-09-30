"""Gold: a small star schema plus one mart.

Dimensions are small and rebuilt each run. The fact table is MERGEd on its grain
(order_id, order_item_id) and only rows whose content changed are rewritten, so a late item
lands under its original order date and an order that moves to "delivered" updates in place
(ADR-0004). At this data size a full-source MERGE is cheap; at scale the source would be
limited to changed keys via Delta Change Data Feed.
"""

from __future__ import annotations

import datetime as dt

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .config import Config

FACT_KEYS = ["order_id", "order_item_id"]
NOT_SOLD = ["canceled", "unavailable"]


def _overwrite(df: DataFrame, name: str) -> None:
    # CREATE OR REPLACE keeps table history (time travel) and handles schema changes atomically.
    df.writeTo(name).using("delta").createOrReplace()


# ---------------------------------------------------------------------------
# pure builders (unit-tested)
# ---------------------------------------------------------------------------
def build_dim_date(spark: SparkSession, start: dt.date = dt.date(2016, 1, 1), end: dt.date = dt.date(2019, 12, 31)) -> DataFrame:
    days = spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}', INTERVAL 1 DAY)) AS date")
    return days.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
        F.dayofweek("date").alias("day_of_week"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )


def build_dim_customer(customers: DataFrame, orders: DataFrame) -> DataFrame:
    """One row per real person (customer_unique_id), not per Olist customer_id (ADR-0007).

    Olist mints a new customer_id for every order, so counting customer_id counts orders.
    Attributes come from the person's most recent order.
    """
    c = customers.join(orders.select("customer_id", "order_purchase_ts"), "customer_id", "left")
    latest_first = Window.partitionBy("customer_unique_id").orderBy(F.col("order_purchase_ts").desc_nulls_last())
    ranked = c.withColumn("_rn", F.row_number().over(latest_first))
    stats = c.groupBy("customer_unique_id").agg(
        F.countDistinct("customer_id").alias("order_count"),
        F.min("order_purchase_ts").alias("first_order_ts"),
    )
    latest = ranked.where("_rn = 1").select(
        "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"
    )
    return latest.join(stats, "customer_unique_id")


def build_fact_order_item(items: DataFrame, orders: DataFrame, customers: DataFrame, seller_history: DataFrame) -> DataFrame:
    o = orders.select(
        "order_id", "customer_id", "order_status", "order_delivered_customer_ts", "order_estimated_delivery_ts"
    )
    c = customers.select("customer_id", "customer_unique_id")
    h = seller_history.select("seller_id", "seller_sk", "valid_from", "valid_to")
    order_date = F.to_date("order_purchase_ts")

    # Items can arrive before their order's first CDC row; they are kept with status 'unknown'
    # and fill in on a later run.
    f = items.join(o, "order_id", "left").join(c, "customer_id", "left")
    # Point-in-time seller: the version that was valid on the order date.
    f = f.join(
        h,
        (f.seller_id == h.seller_id)
        & (order_date >= h.valid_from)
        & (h.valid_to.isNull() | (order_date < h.valid_to)),
        "left",
    ).drop(h.seller_id)

    delivered = F.col("order_delivered_customer_ts").isNotNull()
    out = f.select(
        "order_id",
        "order_item_id",
        F.date_format(order_date, "yyyyMMdd").cast("int").alias("order_date_key"),
        "customer_unique_id",
        "product_id",
        "seller_id",
        "seller_sk",
        F.coalesce("order_status", F.lit("unknown")).alias("order_status"),
        "order_purchase_ts",
        "order_delivered_customer_ts",
        "order_estimated_delivery_ts",
        "price",
        "freight_value",
        F.col("order_status").isin(NOT_SOLD).alias("is_cancelled"),
        delivered.alias("is_delivered"),
        F.when(delivered, F.to_date("order_delivered_customer_ts") <= F.to_date("order_estimated_delivery_ts")).alias("is_on_time"),
        "arrival_lag_days",
    )
    content = [c for c in out.columns if c not in FACT_KEYS]
    return out.withColumn("row_hash", F.sha2(F.to_json(F.struct(*content)), 256))


def build_mart_seller_delivery(fact: DataFrame, reviews: DataFrame, dim_seller: DataFrame) -> DataFrame:
    """Seller x month: GMV, on-time delivery rate and review score."""
    order_score = reviews.groupBy("order_id").agg(F.avg("review_score").alias("order_score"))
    current = dim_seller.where("is_current").select("seller_id", "seller_city", "seller_state")
    f = fact.withColumn("year_month", F.date_format("order_purchase_ts", "yyyy-MM")).join(order_score, "order_id", "left")
    sold = ~F.coalesce(F.col("is_cancelled"), F.lit(False))
    orders_per_seller = f.groupBy("seller_id", "year_month", "order_id").agg(
        F.max(F.when(sold, 1).otherwise(0)).alias("sold"),
        F.max(F.col("is_delivered").cast("int")).alias("delivered"),
        F.max(F.col("is_on_time").cast("int")).alias("on_time"),
        F.first("order_score", ignorenulls=True).alias("order_score"),
    )
    per_order = orders_per_seller.groupBy("seller_id", "year_month").agg(
        F.sum("sold").alias("orders"),
        F.sum("delivered").alias("delivered_orders"),
        F.sum("on_time").alias("on_time_orders"),
        F.round(F.avg("order_score"), 2).alias("avg_review_score"),
    )
    money = f.where(sold).groupBy("seller_id", "year_month").agg(
        F.sum("price").alias("gmv"), F.sum("freight_value").alias("freight"), F.count("*").alias("items")
    )
    return (
        per_order.join(money, ["seller_id", "year_month"], "left")
        .join(current, "seller_id", "left")
        .withColumn("on_time_rate", F.round(F.col("on_time_orders") / F.col("delivered_orders"), 4))
        .select(
            "seller_id", "seller_city", "seller_state", "year_month", "orders", "items", "gmv", "freight",
            "delivered_orders", "on_time_orders", "on_time_rate", "avg_review_score",
        )
    )


# ---------------------------------------------------------------------------
def merge_fact(spark: SparkSession, name: str, source: DataFrame) -> None:
    if not spark.catalog.tableExists(name):
        source.limit(0).withColumn("_updated_at", F.current_timestamp()).write.format("delta").saveAsTable(name)
    src = source.withColumn("_updated_at", F.current_timestamp())
    (
        DeltaTable.forName(spark, name).alias("t")
        .merge(src.alias("s"), " AND ".join(f"t.{k} = s.{k}" for k in FACT_KEYS))
        .whenMatchedUpdateAll(condition="t.row_hash <> s.row_hash")
        .whenNotMatchedInsertAll()
        .execute()
    )


def run(spark: SparkSession, cfg: Config) -> None:
    silver = {n: spark.table(cfg.table("silver", n)) for n in ["order_items", "orders", "customers", "products", "reviews", "seller_history"]}

    if not spark.catalog.tableExists(cfg.table("gold", "dim_date")):
        _overwrite(build_dim_date(spark), cfg.table("gold", "dim_date"))
    _overwrite(build_dim_customer(silver["customers"], silver["orders"]), cfg.table("gold", "dim_customer"))
    _overwrite(silver["products"].drop("_batch_date", "_source_file", "_ingested_at"), cfg.table("gold", "dim_product"))
    _overwrite(silver["seller_history"].drop("attr_hash"), cfg.table("gold", "dim_seller"))

    fact = build_fact_order_item(silver["order_items"], silver["orders"], silver["customers"], silver["seller_history"])
    merge_fact(spark, cfg.table("gold", "fact_order_item"), fact)


def run_mart(spark: SparkSession, cfg: Config) -> None:
    mart = build_mart_seller_delivery(
        spark.table(cfg.table("gold", "fact_order_item")),
        spark.table(cfg.table("silver", "reviews")),
        spark.table(cfg.table("gold", "dim_seller")),
    )
    _overwrite(mart, cfg.table("gold", "mart_seller_delivery_performance"))
