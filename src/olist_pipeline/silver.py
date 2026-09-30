"""Silver: validated, de-duplicated, current-state tables.

Each table is fed incrementally from its bronze table (readStream + foreachBatch +
availableNow). Inside a micro-batch the steps are always: evaluate rules -> quarantine bad
rows -> de-duplicate -> MERGE. MERGE makes every step idempotent, so a replayed file or a
rerun job cannot create duplicates (ADR-0001), and the CDC merge only moves an order forward
(ADR-0002).
"""

from __future__ import annotations

from typing import Callable

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from . import quality as dq
from .config import Config
from .contracts import CUSTOMERS, ORDER_ITEMS, ORDERS_CDC, PRODUCTS, REVIEWS, Feed

LINEAGE = ["_batch_date", "_source_file", "_ingested_at"]


# ---------------------------------------------------------------------------
# pure transformations (unit-tested)
# ---------------------------------------------------------------------------
def keep_first(df: DataFrame, keys: list[str], order_by: list) -> DataFrame:
    """Keep one row per key: the first by `order_by`."""
    w = Window.partitionBy(*keys).orderBy(*order_by)
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def latest_change_per_order(df: DataFrame) -> DataFrame:
    """Several changes for one order can arrive together (and out of order): keep the highest change_seq."""
    return keep_first(df, ["order_id"], [F.col("change_seq").desc(), F.col("_ingested_at").desc()])


def add_lateness(items: DataFrame, cfg: Config) -> DataFrame:
    """Rows are dated by when the sale happened (event_date), never by when the file arrived (ADR-0004)."""
    event_date = F.to_date("order_purchase_ts")
    return items.withColumn("event_date", event_date).withColumn(
        "arrival_lag_days",
        F.when(F.col("_batch_date") > F.lit(cfg.backfill_date), F.datediff("_batch_date", event_date)).otherwise(F.lit(0)),
    )


def duplicate_metrics(before: DataFrame, after: DataFrame, already_loaded: DataFrame, table_name: str, run_id: str) -> DataFrame:
    """Info-level metrics: duplicates removed inside the batch and rows that were already loaded earlier."""
    b = before.groupBy("_batch_date").agg(F.count("*").alias("total_rows"))
    a = after.groupBy("_batch_date").agg(F.count("*").alias("after_rows"))
    l = already_loaded.groupBy("_batch_date").agg(F.count("*").alias("loaded_rows"))
    joined = b.join(a, "_batch_date", "left").join(l, "_batch_date", "left").fillna(0)
    rows = F.explode(F.array(
        F.struct(F.lit("duplicate_in_batch").alias("rule"), (F.col("total_rows") - F.col("after_rows")).alias("failed_rows")),
        F.struct(F.lit("duplicate_already_loaded").alias("rule"), F.col("loaded_rows").alias("failed_rows")),
    ))
    return joined.select(
        F.lit(run_id).alias("run_id"),
        F.lit(table_name).alias("table_name"),
        F.col("_batch_date").alias("batch_date"),
        rows.alias("m"),
        "total_rows",
    ).select(
        "run_id", "table_name", "batch_date", "m.rule", F.lit("info").alias("severity"),
        F.col("m.failed_rows").cast("long").alias("failed_rows"), "total_rows", F.current_timestamp().alias("measured_at"),
    )


# ---------------------------------------------------------------------------
# plumbing
# ---------------------------------------------------------------------------
def ensure_table(spark: SparkSession, name: str, like: DataFrame) -> DeltaTable:
    if not spark.catalog.tableExists(name):
        like.limit(0).write.format("delta").saveAsTable(name)
    return DeltaTable.forName(spark, name)


def merge_insert_new(spark: SparkSession, name: str, rows: DataFrame, keys: list[str]) -> None:
    """Append-only feeds: a key we already hold is a re-delivery, never an update."""
    t = ensure_table(spark, name, rows)
    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    t.alias("t").merge(rows.alias("s"), cond).whenNotMatchedInsertAll().execute()


def merge_upsert(spark: SparkSession, name: str, rows: DataFrame, keys: list[str]) -> None:
    t = ensure_table(spark, name, rows)
    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    t.alias("t").merge(rows.alias("s"), cond).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


def merge_cdc_forward_only(spark: SparkSession, name: str, latest: DataFrame) -> None:
    """Apply one row per order; a change with a lower or equal change_seq than what we hold is ignored."""
    t = ensure_table(spark, name, latest)
    (
        t.alias("t").merge(latest.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll(condition="s.change_seq > t.change_seq")
        .whenNotMatchedInsertAll()
        .execute()
    )


def _contract_cols(feed: Feed) -> list[str]:
    return [f.name for f in feed.schema.fields]


def _stream(spark: SparkSession, cfg: Config, source: str, name: str, fn: Callable[[DataFrame, int], None]) -> None:
    (
        spark.readStream.table(cfg.table("bronze", source))
        .writeStream.foreachBatch(fn)
        .option("checkpointLocation", cfg.checkpoint(f"silver_{name}"))
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )


# ---------------------------------------------------------------------------
# tables
# ---------------------------------------------------------------------------
def products(spark: SparkSession, cfg: Config) -> None:
    cols = _contract_cols(PRODUCTS)

    def fn(mb: DataFrame, _: int) -> None:
        s = mb.sparkSession
        good = dq.record(s, cfg, dq.evaluate(mb, dq.snapshot_rules("product_id")), dq.snapshot_rules("product_id"), "products", cols)
        latest = keep_first(good, ["product_id"], [F.col("_batch_date").desc()]).select(*cols, *LINEAGE)
        merge_upsert(s, cfg.table("silver", "products"), latest, ["product_id"])

    _stream(spark, cfg, "products", "products", fn)


def order_items(spark: SparkSession, cfg: Config) -> None:
    cols = _contract_cols(ORDER_ITEMS)
    rules = dq.item_rules(cfg)
    keys = list(ORDER_ITEMS.keys)
    target_name = cfg.table("silver", "order_items")

    def fn(mb: DataFrame, _: int) -> None:
        s = mb.sparkSession
        known = s.table(cfg.table("silver", "products")).select("product_id", F.lit(True).alias("_product_known"))
        looked_up = mb.join(F.broadcast(known), "product_id", "left").fillna({"_product_known": False})
        good = dq.record(s, cfg, dq.evaluate(looked_up, rules), rules, "order_items", cols)

        deduped = keep_first(good, keys, [F.col("_ingested_at"), F.col("_source_file")])
        rows = add_lateness(deduped, cfg).select(*cols, "event_date", "arrival_lag_days", *LINEAGE)
        t = ensure_table(s, target_name, rows)

        already = rows.join(t.toDF().select(*keys), keys, "left_semi")
        duplicate_metrics(good, deduped, already, "order_items", cfg.run_id).write.format("delta").mode("append").saveAsTable(
            cfg.table("ops", "dq_metrics")
        )
        merge_insert_new(s, target_name, rows, keys)

    _stream(spark, cfg, "order_items", "order_items", fn)


def orders(spark: SparkSession, cfg: Config) -> None:
    cols = _contract_cols(ORDERS_CDC)
    rules = dq.order_rules()

    def fn(mb: DataFrame, _: int) -> None:
        s = mb.sparkSession
        good = dq.record(s, cfg, dq.evaluate(mb, rules), rules, "orders_cdc", cols)
        latest = latest_change_per_order(good).select(*cols, *LINEAGE)
        merge_cdc_forward_only(s, cfg.table("silver", "orders"), latest)

    _stream(spark, cfg, "orders_cdc", "orders", fn)


def customers(spark: SparkSession, cfg: Config) -> None:
    cols = _contract_cols(CUSTOMERS)
    rules = dq.customer_rules()

    def fn(mb: DataFrame, _: int) -> None:
        s = mb.sparkSession
        good = dq.record(s, cfg, dq.evaluate(mb, rules), rules, "customers", cols)
        latest = keep_first(good, ["customer_id"], [F.col("_ingested_at").desc()]).select(*cols, *LINEAGE)
        merge_upsert(s, cfg.table("silver", "customers"), latest, ["customer_id"])

    _stream(spark, cfg, "customers", "customers", fn)


def reviews(spark: SparkSession, cfg: Config) -> None:
    cols = _contract_cols(REVIEWS)
    rules = dq.review_rules()
    keys = list(REVIEWS.keys)

    def fn(mb: DataFrame, _: int) -> None:
        s = mb.sparkSession
        good = dq.record(s, cfg, dq.evaluate(mb, rules), rules, "reviews", cols)
        latest = keep_first(good, keys, [F.col("review_answer_ts").desc_nulls_last()]).select(*cols, *LINEAGE)
        merge_upsert(s, cfg.table("silver", "reviews"), latest, keys)

    _stream(spark, cfg, "reviews", "reviews", fn)


def run(spark: SparkSession, cfg: Config) -> None:
    products(spark, cfg)  # order_items checks product_id against it
    order_items(spark, cfg)
    orders(spark, cfg)
    customers(spark, cfg)
    reviews(spark, cfg)
