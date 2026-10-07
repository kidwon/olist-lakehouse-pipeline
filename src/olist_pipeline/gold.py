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
NOT_YET = -1  # date key of the dim_date row for a milestone that has not happened (yet)


def build_dim_date(spark: SparkSession, start: dt.date = dt.date(2016, 1, 1), end: dt.date = dt.date(2019, 12, 31)) -> DataFrame:
    """One row per calendar day, plus the special row NOT_YET.

    Kimball: a fact's date foreign key should never be NULL. An accumulating snapshot has
    milestones that have not happened yet, so they point at this row instead (ADR-0009).
    """
    days = spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}', INTERVAL 1 DAY)) AS date")
    calendar = days.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.date_format("date", "yyyy-MM-dd").alias("date_label"),
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
        F.dayofweek("date").alias("day_of_week"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )
    not_yet = spark.range(1).select(
        F.lit(NOT_YET).alias("date_key"),
        F.lit(None).cast("date").alias("date"),
        F.lit("not yet happened").alias("date_label"),
        F.lit(None).cast("int").alias("year"),
        F.lit(None).cast("int").alias("month"),
        F.lit("not yet happened").alias("year_month"),
        F.lit(None).cast("int").alias("day_of_week"),
        F.lit(None).cast("boolean").alias("is_weekend"),
    )
    return calendar.unionByName(not_yet)


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


FULFILLMENT_STAGES = [
    # (duration column, from milestone, to milestone)
    ("hours_to_approve", "purchase_ts", "approved_ts"),
    ("hours_to_ship", "approved_ts", "shipped_ts"),
    ("hours_in_transit", "shipped_ts", "delivered_ts"),
    ("hours_total", "purchase_ts", "delivered_ts"),
]


def _hours(start: str, end: str):
    return F.round((F.unix_timestamp(end) - F.unix_timestamp(start)) / 3600, 2)


def _date_key(ts: str):
    """yyyyMMdd key into dim_date; a milestone that has not happened gets NOT_YET, never NULL."""
    return F.coalesce(F.date_format(ts, "yyyyMMdd").cast("int"), F.lit(NOT_YET))


def build_fact_order_fulfillment(
    changes: DataFrame, items: DataFrame, customers: DataFrame, reviews: DataFrame | None = None
) -> DataFrame:
    """Accumulating snapshot: one row per order, updated in place as milestones arrive (ADR-0009).

    Milestone times come from the order's latest change (an after-image carries every timestamp
    known so far); the end of an unfulfilled order is the first cancelled/unavailable change.
    A stage duration that comes out negative (inconsistent source timestamps) is set to NULL and
    the order is flagged; the raw timestamps are kept as delivered.
    """
    latest = (
        changes.withColumn("_rn", F.row_number().over(Window.partitionBy("order_id").orderBy(F.col("change_seq").desc())))
        .where("_rn = 1")
        .select(
            "order_id",
            "customer_id",
            F.col("order_status").alias("current_status"),
            F.col("order_purchase_ts").alias("purchase_ts"),
            F.col("order_approved_ts").alias("approved_ts"),
            F.col("order_delivered_carrier_ts").alias("shipped_ts"),
            F.col("order_delivered_customer_ts").alias("delivered_ts"),
            F.col("order_estimated_delivery_ts").alias("estimated_ts"),
            F.col("change_seq").alias("last_change_seq"),
        )
    )
    ended = (
        changes.where(F.col("order_status").isin(NOT_SOLD))
        .withColumn("_rn", F.row_number().over(Window.partitionBy("order_id").orderBy("change_ts", "change_seq")))
        .where("_rn = 1")
        .select("order_id", F.col("change_ts").alias("ended_ts"), F.col("order_status").alias("end_reason"))
    )
    lines = items.groupBy("order_id").agg(F.count("*").alias("item_count"), F.sum("price").alias("order_value"))
    who = customers.select("customer_id", "customer_unique_id")
    # Satisfaction next to the delay: an order can have several reviews, so their mean is used.
    if reviews is None:
        reviews = changes.sparkSession.createDataFrame([], "order_id string, review_score int")
    scores = reviews.groupBy("order_id").agg(
        F.round(F.avg("review_score"), 2).alias("review_score"), F.count("*").alias("review_count")
    )

    f = (
        latest.join(ended, "order_id", "left").join(lines, "order_id", "left").join(who, "customer_id", "left")
        .join(scores, "order_id", "left")
    )

    raw = {name: _hours(a, b) for name, a, b in FULFILLMENT_STAGES}
    inconsistent = F.lit(False)
    for expr in raw.values():
        inconsistent = inconsistent | F.coalesce(expr < 0, F.lit(False))

    delivered = F.col("delivered_ts").isNotNull()
    # The estimate is a date: delivering any time that day is on time, so lateness counts from its end.
    f = f.withColumn("_estimated_day_end", F.date_add(F.to_date("estimated_ts"), 1).cast("timestamp"))
    out = f.select(
        "order_id",
        "customer_unique_id",
        "current_status",
        "end_reason",
        _date_key("purchase_ts").alias("purchase_date_key"),
        _date_key("approved_ts").alias("approved_date_key"),
        _date_key("shipped_ts").alias("shipped_date_key"),
        _date_key("delivered_ts").alias("delivered_date_key"),
        _date_key("estimated_ts").alias("estimated_date_key"),
        "purchase_ts", "approved_ts", "shipped_ts", "delivered_ts", "estimated_ts", "ended_ts",
        *[F.when(expr >= 0, expr).alias(name) for name, expr in raw.items()],
        F.when(delivered, _hours("_estimated_day_end", "delivered_ts")).alias("hours_late"),
        F.when(delivered, F.to_date("delivered_ts") <= F.to_date("estimated_ts")).alias("is_on_time"),
        inconsistent.alias("has_inconsistent_milestones"),
        F.coalesce("item_count", F.lit(0)).cast("int").alias("item_count"),
        "order_value",
        "review_score",
        F.coalesce("review_count", F.lit(0)).cast("int").alias("review_count"),
        "last_change_seq",
    )
    content = [c for c in out.columns if c != "order_id"]
    return out.withColumn("row_hash", F.sha2(F.to_json(F.struct(*content)), 256))


def inconsistent_milestone_metric(fact: DataFrame, run_id: str) -> DataFrame:
    """Warn-level metric for the dashboard; the gate only reads `__all__`, so it never blocks a run."""
    return fact.agg(
        F.count_if("has_inconsistent_milestones").alias("failed_rows"), F.count("*").alias("total_rows")
    ).select(
        F.lit(run_id).alias("run_id"),
        F.lit("fact_order_fulfillment").alias("table_name"),
        F.lit(None).cast("date").alias("batch_date"),
        F.lit("inconsistent_milestones").alias("rule"),
        F.lit("warn").alias("severity"),
        F.col("failed_rows").cast("long"),
        F.col("total_rows").cast("long"),
        F.current_timestamp().alias("measured_at"),
    )


OPEN_STATUSES = ["created", "approved", "invoiced", "processing", "shipped"]
BACKLOG_KEYS = ["snapshot_date_key", "order_status", "customer_state"]
STALE_AFTER_DAYS = 30


def build_fact_daily_order_backlog(changes: DataFrame, items: DataFrame, customers: DataFrame) -> DataFrame:
    """Periodic snapshot: open orders per day x status x customer state, by event time (ADR-0010).

    An order's status on day D is the status of its highest `change_seq` among the changes dated
    on or before D. Every day from the purchase date to the last day in the log is counted; an
    order leaves the backlog once it is delivered, cancelled or unavailable. Because the day is
    the day the change *happened*, a late change restates past days.
    """
    end_date = changes.agg(F.max(F.to_date("change_ts")).alias("d")).first()["d"]
    # The last change of each order on each day (several changes can happen on one day).
    per_day = (
        changes.withColumn("change_date", F.to_date("change_ts"))
        .withColumn("_rn", F.row_number().over(Window.partitionBy("order_id", "change_date").orderBy(F.col("change_seq").desc())))
        .where("_rn = 1")
        .select("order_id", "customer_id", "change_date", "change_seq",
                F.to_date("order_purchase_ts").alias("purchase_date"), F.to_date("order_estimated_delivery_ts").alias("estimated_date"))
    )
    by_date = Window.partitionBy("order_id").orderBy("change_date")
    # From each change date on, the order is in the state of the highest sequence seen so far.
    segments = per_day.withColumn(
        "state_seq", F.max("change_seq").over(by_date.rowsBetween(Window.unboundedPreceding, Window.currentRow))
    ).withColumn("segment_end", F.coalesce(F.date_sub(F.lead("change_date").over(by_date), 1), F.lit(end_date)))
    status_of = changes.select("order_id", F.col("change_seq").alias("state_seq"), "order_status")
    segments = segments.join(status_of, ["order_id", "state_seq"]).where(F.col("order_status").isin(OPEN_STATUSES))

    # One row per open order per day. A change dated before the purchase (inconsistent source
    # data) never makes an order count before it existed.
    start = F.greatest("change_date", "purchase_date")
    daily = segments.where(start <= F.col("segment_end")).select(
        "order_id", "customer_id", "order_status", "purchase_date", "estimated_date",
        F.explode(F.sequence(start, F.col("segment_end"))).alias("snapshot_date"),
    )
    value = items.groupBy("order_id").agg(F.sum("price").alias("order_value"))
    state = customers.select("customer_id", "customer_state")
    daily = daily.join(value, "order_id", "left").join(state, "customer_id", "left")

    age = F.datediff("snapshot_date", "purchase_date")
    out = daily.groupBy(
        "snapshot_date",
        "order_status",
        # A merge key must never be NULL, or the row would never match and duplicate every run.
        F.coalesce("customer_state", F.lit("unknown")).alias("customer_state"),
    ).agg(
        F.count("*").alias("open_orders"),
        F.coalesce(F.sum("order_value"), F.lit(0)).alias("open_order_value"),
        F.percentile_approx(age, 0.5).alias("median_age_days"),
        F.count_if(age > STALE_AFTER_DAYS).alias("orders_open_over_30_days"),
        F.count_if(F.col("snapshot_date") > F.col("estimated_date")).alias("overdue_orders"),
    ).select(_date_key("snapshot_date").alias("snapshot_date_key"), "*")
    content = [c for c in out.columns if c not in BACKLOG_KEYS]
    return out.withColumn("row_hash", F.sha2(F.to_json(F.struct(*content)), 256))


# ---------------------------------------------------------------------------
def merge_fact(
    spark: SparkSession, name: str, source: DataFrame, keys: list[str] = FACT_KEYS, delete_missing: bool = False
) -> None:
    """MERGE on the grain; rewrite a row only when its content hash changed.

    `delete_missing` removes target rows absent from the source: for a snapshot rebuilt from the
    full history, a combination that no longer exists (e.g. a backlog that dropped to zero after a
    late change) must disappear.
    """
    if not spark.catalog.tableExists(name):
        source.limit(0).withColumn("_updated_at", F.current_timestamp()).write.format("delta").saveAsTable(name)
    src = source.withColumn("_updated_at", F.current_timestamp())
    merge = (
        DeltaTable.forName(spark, name).alias("t")
        .merge(src.alias("s"), " AND ".join(f"t.{k} = s.{k}" for k in keys))
        .whenMatchedUpdateAll(condition="t.row_hash <> s.row_hash")
        .whenNotMatchedInsertAll()
    )
    if delete_missing:
        merge = merge.whenNotMatchedBySourceDelete()
    merge.execute()


def run(spark: SparkSession, cfg: Config) -> None:
    silver = {n: spark.table(cfg.table("silver", n)) for n in ["order_items", "orders", "customers", "products", "reviews", "seller_history"]}

    # Rebuilt every run (about 1,500 rows), so a change to the dimension reaches existing deployments.
    _overwrite(build_dim_date(spark), cfg.table("gold", "dim_date"))
    _overwrite(build_dim_customer(silver["customers"], silver["orders"]), cfg.table("gold", "dim_customer"))
    _overwrite(silver["products"].drop("_batch_date", "_source_file", "_ingested_at"), cfg.table("gold", "dim_product"))
    _overwrite(silver["seller_history"].drop("attr_hash"), cfg.table("gold", "dim_seller"))

    fact = build_fact_order_item(silver["order_items"], silver["orders"], silver["customers"], silver["seller_history"])
    merge_fact(spark, cfg.table("gold", "fact_order_item"), fact)

    fulfillment = build_fact_order_fulfillment(
        spark.table(cfg.table("silver", "order_changes")), silver["order_items"], silver["customers"], silver["reviews"]
    )
    merge_fact(spark, cfg.table("gold", "fact_order_fulfillment"), fulfillment, ["order_id"])
    inconsistent_milestone_metric(spark.table(cfg.table("gold", "fact_order_fulfillment")), cfg.run_id).write.format(
        "delta"
    ).mode("append").saveAsTable(cfg.table("ops", "dq_metrics"))

    backlog = build_fact_daily_order_backlog(
        spark.table(cfg.table("silver", "order_changes")), silver["order_items"], silver["customers"]
    )
    merge_fact(spark, cfg.table("gold", "fact_daily_order_backlog"), backlog, BACKLOG_KEYS, delete_missing=True)


def run_mart(spark: SparkSession, cfg: Config) -> None:
    mart = build_mart_seller_delivery(
        spark.table(cfg.table("gold", "fact_order_item")),
        spark.table(cfg.table("silver", "reviews")),
        spark.table(cfg.table("gold", "dim_seller")),
    )
    _overwrite(mart, cfg.table("gold", "mart_seller_delivery_performance"))
