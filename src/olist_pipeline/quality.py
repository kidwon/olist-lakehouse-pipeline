"""Data quality: named rules, quarantine and per-batch metrics (ADR-0005).

A rule is a name plus a condition that is TRUE when a row is bad. Every row gets the list of
rules it breaks; rows with an empty list continue, the others go to `ops.quarantine` with the
reasons attached. Nothing is dropped silently: every rule's hit count per batch lands in
`ops.dq_metrics`, which the gate and the dashboard read.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Config

ALL_RULES = "__all__"  # metric row counting rows that broke at least one rule
RESCUED = "rescued_data"  # warning metric: fields outside the contract (not a failure)

ORDER_STATUSES = ["created", "approved", "invoiced", "processing", "shipped", "delivered", "canceled", "unavailable"]


@dataclass(frozen=True)
class Rule:
    name: str
    fails: Column  # TRUE when the row is bad


def missing(*cols: str) -> Column:
    cond = F.lit(False)
    for c in cols:
        cond = cond | F.col(c).isNull()
    return cond


def item_rules(cfg: Config) -> list[Rule]:
    lateness = F.datediff(F.col("_batch_date"), F.to_date("order_purchase_ts"))
    return [
        Rule("malformed_record", F.col("_malformed")),
        Rule("missing_key", missing("order_id", "order_item_id")),
        Rule("price_not_positive", F.col("price").isNull() | (F.col("price") <= 0)),
        Rule("freight_negative", F.col("freight_value") < 0),
        Rule("missing_product_id", F.col("product_id").isNull()),
        # `_product_known` is added by a lookup against silver.products before the rules run.
        Rule("unknown_product_id", F.col("product_id").isNotNull() & ~F.col("_product_known")),
        # The backfill batch carries years of history by design, so lateness only applies afterwards.
        Rule("too_late", (F.col("_batch_date") > F.lit(cfg.backfill_date)) & (lateness > cfg.late_tolerance_days)),
    ]


def order_rules() -> list[Rule]:
    return [
        Rule("malformed_record", F.col("_malformed")),
        Rule("missing_key", missing("order_id", "change_seq", "change_ts")),
        Rule("unknown_status", ~F.col("order_status").isin(ORDER_STATUSES)),
        Rule("delivered_before_purchase", F.col("order_delivered_customer_ts") < F.col("order_purchase_ts")),
        # Real Olist data: ~0.17% of orders have a carrier date before the purchase (e.g. 2018-01 vs 2018-07),
        # which would make the order appear months before it was placed.
        Rule("change_before_purchase", F.col("change_ts") < F.col("order_purchase_ts")),
    ]


def customer_rules() -> list[Rule]:
    return [
        Rule("malformed_record", F.col("_malformed")),
        Rule("missing_key", missing("customer_id", "customer_unique_id")),
    ]


def review_rules() -> list[Rule]:
    return [
        Rule("malformed_record", F.col("_malformed")),
        Rule("missing_key", missing("review_id", "order_id")),
        Rule("score_out_of_range", ~F.col("review_score").between(1, 5)),
    ]


def snapshot_rules(key: str) -> list[Rule]:
    return [Rule("missing_key", missing(key))]


# ---------------------------------------------------------------------------
def evaluate(df: DataFrame, rules: list[Rule]) -> DataFrame:
    """Add `_failed_rules`: the names of every rule the row breaks (NULL conditions count as pass)."""
    flags = F.array(*[F.when(F.coalesce(r.fails, F.lit(False)), F.lit(r.name)) for r in rules])
    return df.withColumn("_failed_rules", F.filter(flags, lambda x: x.isNotNull()))


def split(evaluated: DataFrame) -> tuple[DataFrame, DataFrame]:
    bad = F.size("_failed_rules") > 0
    return evaluated.where(~bad), evaluated.where(bad)


def metrics(evaluated: DataFrame, rules: list[Rule], table_name: str, run_id: str) -> DataFrame:
    """One row per (batch_date, rule) with failed and total row counts, plus __all__ and rescued_data."""
    names = [r.name for r in rules]
    has_rescued = "_rescued_data" in evaluated.columns
    agg = evaluated.groupBy("_batch_date").agg(
        F.count("*").alias("total_rows"),
        F.count_if(F.size("_failed_rules") > 0).alias(ALL_RULES),
        *[F.count_if(F.array_contains("_failed_rules", n)).alias(n) for n in names],
        (F.count_if(F.col("_rescued_data").isNotNull()) if has_rescued else F.lit(0).cast("long")).alias(RESCUED),
    )
    stack_cols = [ALL_RULES, *names, RESCUED]
    stack = F.explode(F.array(*[
        F.struct(
            F.lit(n).alias("rule"),
            F.lit("warn" if n == RESCUED else "error").alias("severity"),
            F.col(f"`{n}`").cast("long").alias("failed_rows"),
        )
        for n in stack_cols
    ]))
    return agg.select(
        F.lit(run_id).alias("run_id"),
        F.lit(table_name).alias("table_name"),
        F.col("_batch_date").alias("batch_date"),
        stack.alias("m"),
        "total_rows",
    ).select("run_id", "table_name", "batch_date", "m.*", "total_rows", F.current_timestamp().alias("measured_at"))


def quarantine_rows(bad: DataFrame, contract_cols: list[str], table_name: str, run_id: str) -> DataFrame:
    return bad.select(
        F.lit(run_id).alias("run_id"),
        F.lit(table_name).alias("table_name"),
        F.col("_batch_date").alias("batch_date"),
        F.col("_failed_rules").alias("failed_rules"),
        F.to_json(F.struct(*contract_cols)).alias("record"),
        F.col("_raw").alias("raw"),
        F.col("_source_file").alias("source_file"),
        F.current_timestamp().alias("quarantined_at"),
    )


def record(spark: SparkSession, cfg: Config, evaluated: DataFrame, rules: list[Rule], table_name: str, contract_cols: list[str]) -> DataFrame:
    """Write metrics and quarantine for one micro-batch; return the rows that passed."""
    # No persist(): serverless compute does not support caching; a micro-batch is cheap to recompute.
    good, bad = split(evaluated)
    metrics(evaluated, rules, table_name, cfg.run_id).write.format("delta").mode("append").saveAsTable(cfg.table("ops", "dq_metrics"))
    quarantine_rows(bad, contract_cols, table_name, cfg.run_id).write.format("delta").mode("append").saveAsTable(
        cfg.table("ops", "quarantine")
    )
    return good
