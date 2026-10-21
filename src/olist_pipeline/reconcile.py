"""Reconcile the imperative and declarative implementations, table by table (ADR-0013).

Both implementations read the same landing files and must produce the same tables. The
declarative pipeline cannot run outside Databricks, so this daily check is its test: the
imperative version, unit- and end-to-end-tested locally, is the reference. A check counts the
rows found on one side only (EXCEPT ALL, so duplicates count); any difference fails the job.

Columns that are implementation details are left out: lineage (_batch_date, _ingested_at, ...),
the content hash and update time the imperative MERGE needs, and SCD2 bookkeeping.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Config
from .contracts import CUSTOMERS, ORDER_ITEMS, ORDERS_CDC, PRODUCTS, REVIEWS


class ReconciliationError(RuntimeError):
    pass


@dataclass(frozen=True)
class Check:
    name: str
    imperative: tuple[str, str]  # (layer, table)
    sdp: str  # table in the declarative pipeline's schema
    columns: tuple[str, ...]
    # Ops tables append again when the imperative pipeline reprocesses a batch (e.g. a
    # re-delivered file), so they are compared as sets; business tables must match row for row.
    as_set: bool = False
    where: str | None = None


def _cols(feed) -> tuple[str, ...]:
    return tuple(f.name for f in feed.schema.fields)


def _without(*drop: str):
    return lambda cols: tuple(c for c in cols if c not in drop)


FACT_ITEM = (
    "order_id",
    "order_item_id",
    "order_date_key",
    "customer_unique_id",
    "product_id",
    "seller_id",
    "seller_sk",
    "order_status",
    "order_purchase_ts",
    "order_delivered_customer_ts",
    "order_estimated_delivery_ts",
    "price",
    "freight_value",
    "is_cancelled",
    "is_delivered",
    "is_on_time",
    "arrival_lag_days",
)
FULFILLMENT = (
    "order_id",
    "customer_unique_id",
    "current_status",
    "end_reason",
    "purchase_date_key",
    "approved_date_key",
    "shipped_date_key",
    "delivered_date_key",
    "estimated_date_key",
    "purchase_ts",
    "approved_ts",
    "shipped_ts",
    "delivered_ts",
    "estimated_ts",
    "ended_ts",
    "hours_to_approve",
    "hours_to_ship",
    "hours_in_transit",
    "hours_total",
    "hours_late",
    "is_on_time",
    "has_inconsistent_milestones",
    "item_count",
    "order_value",
    "review_score",
    "review_count",
    "last_change_seq",
)
BACKLOG = (
    "snapshot_date_key",
    "snapshot_date",
    "order_status",
    "customer_state",
    "open_orders",
    "open_order_value",
    "median_age_days",
    "orders_open_over_30_days",
    "overdue_orders",
)
MART = (
    "seller_id",
    "seller_city",
    "seller_state",
    "year_month",
    "orders",
    "items",
    "gmv",
    "freight",
    "delivered_orders",
    "on_time_orders",
    "on_time_rate",
    "avg_review_score",
)

CHECKS: tuple[Check, ...] = (
    # silver
    Check("silver orders", ("silver", "orders"), "silver_orders", _cols(ORDERS_CDC)),
    Check("silver order changes", ("silver", "order_changes"), "silver_order_changes", _cols(ORDERS_CDC)),
    Check(
        "silver order items",
        ("silver", "order_items"),
        "silver_order_items",
        (*_cols(ORDER_ITEMS), "event_date", "arrival_lag_days"),
    ),
    Check("silver customers", ("silver", "customers"), "silver_customers", _cols(CUSTOMERS)),
    Check("silver reviews", ("silver", "reviews"), "silver_reviews", _cols(REVIEWS)),
    Check("silver products", ("silver", "products"), "silver_products", _cols(PRODUCTS)),
    # gold
    Check(
        "dim_date",
        ("gold", "dim_date"),
        "dim_date",
        ("date_key", "date", "date_label", "year", "month", "year_month", "day_of_week", "is_weekend"),
    ),
    Check(
        "dim_customer",
        ("gold", "dim_customer"),
        "dim_customer",
        (
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
            "order_count",
            "first_order_ts",
        ),
    ),
    Check("dim_product", ("gold", "dim_product"), "dim_product", _cols(PRODUCTS)),
    Check(
        "dim_seller",
        ("gold", "dim_seller"),
        "dim_seller",
        (
            "seller_sk",
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
            "valid_from",
            "valid_to",
            "is_current",
        ),
    ),
    Check("fact_order_item", ("gold", "fact_order_item"), "fact_order_item", FACT_ITEM),
    Check("fact_order_fulfillment", ("gold", "fact_order_fulfillment"), "fact_order_fulfillment", FULFILLMENT),
    Check("fact_daily_order_backlog", ("gold", "fact_daily_order_backlog"), "fact_daily_order_backlog", BACKLOG),
    Check("mart", ("gold", "mart_seller_delivery_performance"), "mart_seller_delivery_performance", MART),
    # ops
    Check(
        "quarantine",
        ("ops", "quarantine"),
        "quarantine",
        ("table_name", "batch_date", "failed_rules", "record"),
        as_set=True,
    ),
    # Rule hits per batch. The de-duplication counts (info) are left out: re-processing a batch
    # legitimately reports its rows again as "already loaded".
    Check(
        "dq_metrics",
        ("ops", "dq_metrics"),
        "dq_metrics",
        ("table_name", "batch_date", "rule", "severity", "failed_rows", "total_rows"),
        as_set=True,
        where="severity IN ('error', 'warn') AND batch_date IS NOT NULL "
        "AND (rule IN ('__all__', 'rescued_data') OR failed_rows > 0)",
    ),
)


def difference(imperative: DataFrame, sdp: DataFrame, as_set: bool = False) -> tuple[int, int]:
    """(rows only in the imperative version, rows only in the declarative one)."""
    if as_set:
        imperative, sdp = imperative.distinct(), sdp.distinct()
    return imperative.exceptAll(sdp).count(), sdp.exceptAll(imperative).count()


def _side(spark: SparkSession, table: str, check: Check) -> DataFrame:
    df = spark.table(table)
    if check.where:
        df = df.where(check.where)
    return df.select(*check.columns)


def evaluate(spark: SparkSession, cfg: Config) -> DataFrame:
    rows = []
    for c in CHECKS:
        only_imp, only_sdp = difference(
            _side(spark, cfg.table(*c.imperative), c), _side(spark, cfg.sdp_table(c.sdp), c), c.as_set
        )
        rows.append((c.name, cfg.table(*c.imperative), cfg.sdp_table(c.sdp), only_imp, only_sdp))
    return (
        spark.createDataFrame(
            rows, "check STRING, imperative_table STRING, sdp_table STRING, only_imperative LONG, only_sdp LONG"
        )
        .withColumn("passed", (F.col("only_imperative") == 0) & (F.col("only_sdp") == 0))
        .withColumn("run_id", F.lit(cfg.run_id))
        .withColumn("checked_at", F.current_timestamp())
    )


def run(spark: SparkSession, cfg: Config) -> None:
    result = evaluate(spark, cfg)
    result.write.format("delta").mode("append").saveAsTable(cfg.table("ops", "reconcile_log"))
    failed = result.where("NOT passed").collect()
    for r in result.collect():
        print(
            f"{'ok  ' if r.passed else 'DIFF'} {r.check:<26} only imperative {r.only_imperative:>7}  only sdp {r.only_sdp:>7}"
        )
    if failed:
        detail = ", ".join(f"{r.check} ({r.only_imperative}/{r.only_sdp})" for r in failed)
        raise ReconciliationError(f"The two implementations differ: {detail}. See {cfg.table('ops', 'reconcile_log')}.")
