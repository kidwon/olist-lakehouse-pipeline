"""Turn the static Olist dump into a stream of daily file drops.

`prepare` reads the raw Kaggle CSVs once and writes every future file drop to a staging area,
each row tagged with the day it will be delivered (`_delivery_date`) and, when it was
tampered with, the anomaly that was injected (`_anomaly`). A per-day summary goes to
`ops.replay_manifest`, which is what tests and the README cite as ground truth.

`deliver` copies one day from staging into the landing zone, exactly like an upstream system
dropping files. The pipeline itself never reads staging or the manifest.

Anomaly selection is hash-based (xxhash64 of the row key + seed), so the same seed always
produces the same dirty data, and the anomaly buckets are disjoint by construction.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Config
from .contracts import FEEDS, MONEY

# Disjoint anomaly buckets on a 0..9999 hash, applied to replay-day rows only.
ITEM_BUCKETS = {
    "duplicate_same_batch": (0, 100),
    "duplicate_next_batch": (100, 150),
    "late_within_tolerance": (150, 300),
    "too_late": (300, 330),
    "invalid_price": (330, 380),
    "missing_product_id": (380, 410),
    "unknown_product_id": (410, 440),
}
POISON_BUCKET = (1000, 2200)  # ~12% of the poisoned day's items
ORDER_BUCKETS = {
    "cdc_duplicate": (0, 100),
    "cdc_out_of_order": (100, 250),
    "invalid_delivery_ts": (250, 300),
}
SELLER_MOVE_BUCKET = (0, 200)  # ~2% of sellers relocate once during the replay
NEW_FIELD = "discount_amount"
NEW_FIELD_AFTER_DAYS = 5  # order_items gains a field from the 6th replay day
RELOCATION_TARGETS = [("curitiba", "PR"), ("porto alegre", "RS"), ("belo horizonte", "MG"), ("recife", "PE")]

SOURCE_FILES = {
    "orders": "olist_orders_dataset.csv",
    "items": "olist_order_items_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "translation": "product_category_name_translation.csv",
}


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _bucket(seed: int, salt: str, *cols: str) -> Column:
    return F.pmod(F.xxhash64(F.lit(seed), F.lit(salt), *[F.col(c) for c in cols]), F.lit(10000))


def _in(bucket: Column, rng: tuple[int, int]) -> Column:
    return (bucket >= rng[0]) & (bucket < rng[1])


def _read_csv(spark: SparkSession, cfg: Config, key: str) -> DataFrame:
    df = (
        spark.read.option("header", True)
        .option("multiLine", True)  # review comments contain line breaks
        .option("escape", '"')
        .csv(f"{cfg.source_path}/{SOURCE_FILES[key]}")
    )
    # product_category_name_translation.csv starts with a UTF-8 BOM, which would otherwise
    # end up in the first column name and silently break the category join.
    return df.toDF(*[c.lstrip("\ufeff") for c in df.columns])


def _ts(c: str) -> Column:
    return F.to_timestamp(F.col(c))


def _delivery_date(event_ts: Column, cfg: Config) -> Column:
    """History before the backfill date arrives in one backfill batch; later rows on their own day."""
    return F.greatest(F.to_date(event_ts), F.lit(cfg.backfill_date))


def _within_horizon(df: DataFrame, cfg: Config) -> DataFrame:
    return df.where(F.col("_delivery_date") <= F.lit(cfg.last_replay_date))


def _shift(days: int, cfg: Config) -> Column:
    """Delay delivery by `days`, but only if the delayed day is still inside the replay window."""
    shifted = F.date_add(F.col("_delivery_date"), days)
    return F.when(shifted <= F.lit(cfg.last_replay_date), shifted)


# ---------------------------------------------------------------------------
# feed builders
# ---------------------------------------------------------------------------
def build_orders_cdc(orders: DataFrame, cfg: Config) -> DataFrame:
    """Explode each order's lifecycle timestamps into one after-image row per status change."""
    o = orders.select(
        "order_id",
        "customer_id",
        F.col("order_status").alias("final_status"),
        _ts("order_purchase_timestamp").alias("purchase"),
        _ts("order_approved_at").alias("approved"),
        _ts("order_delivered_carrier_date").alias("carrier"),
        _ts("order_delivered_customer_date").alias("customer"),
        _ts("order_estimated_delivery_date").alias("estimated"),
    )
    stages = [("created", "purchase", 0), ("approved", "approved", 1), ("shipped", "carrier", 2), ("delivered", "customer", 3)]
    steps = F.array_sort(F.filter(
        F.array(*[
            F.when(F.col(ts).isNotNull(), F.struct(F.col(ts).alias("ts"), F.lit(rank).alias("rank"), F.lit(status).alias("status")))
            for status, ts, rank in stages
        ]),
        lambda s: s.isNotNull(),
    ))
    last = F.element_at(steps, -1)
    # Orders that end cancelled/unavailable/invoiced/... get a final change one hour after the last stamp.
    final_step = F.struct((last["ts"] + F.expr("INTERVAL 1 HOUR")).alias("ts"), F.lit(9).alias("rank"), F.col("final_status").alias("status"))
    steps = F.when(last["status"] == F.col("final_status"), steps).otherwise(F.concat(steps, F.array(final_step)))

    exploded = o.select("*", F.posexplode(steps).alias("pos", "step"))
    change_ts = F.col("step.ts")

    def visible(c: str) -> Column:
        return F.when(F.col(c) <= change_ts, F.col(c))

    return exploded.select(
        "order_id",
        "customer_id",
        F.col("step.status").alias("order_status"),
        F.col("purchase").alias("order_purchase_ts"),
        visible("approved").alias("order_approved_ts"),
        visible("carrier").alias("order_delivered_carrier_ts"),
        visible("customer").alias("order_delivered_customer_ts"),
        F.col("estimated").alias("order_estimated_delivery_ts"),
        (F.col("pos") + 1).cast("int").alias("change_seq"),
        change_ts.alias("change_ts"),
        _delivery_date(change_ts, cfg).alias("_delivery_date"),
    )


def build_order_items(items: DataFrame, orders: DataFrame, cfg: Config) -> DataFrame:
    purchase = orders.select("order_id", _ts("order_purchase_timestamp").alias("order_purchase_ts"))
    return items.join(purchase, "order_id").select(
        "order_id",
        F.col("order_item_id").cast("int").alias("order_item_id"),
        "product_id",
        "seller_id",
        "order_purchase_ts",
        _ts("shipping_limit_date").alias("shipping_limit_ts"),
        F.col("price").cast(MONEY).alias("price"),
        F.col("freight_value").cast(MONEY).alias("freight_value"),
        _delivery_date(F.col("order_purchase_ts"), cfg).alias("_delivery_date"),
    )


def build_customers(customers: DataFrame, orders: DataFrame, cfg: Config) -> DataFrame:
    # In Olist every customer_id belongs to exactly one order; the record arrives with that order.
    first_seen = orders.groupBy("customer_id").agg(F.min(_ts("order_purchase_timestamp")).alias("seen"))
    return customers.join(first_seen, "customer_id").select(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        _delivery_date(F.col("seen"), cfg).alias("_delivery_date"),
    )


def build_reviews(reviews: DataFrame, cfg: Config) -> DataFrame:
    created = _ts("review_creation_date")
    return reviews.select(
        "review_id",
        "order_id",
        F.col("review_score").cast("int").alias("review_score"),
        created.alias("review_creation_ts"),
        _ts("review_answer_timestamp").alias("review_answer_ts"),
        _delivery_date(created, cfg).alias("_delivery_date"),
    ).where(F.col("review_id").isNotNull() & F.col("order_id").isNotNull())


def build_products(products: DataFrame, translation: DataFrame, cfg: Config) -> DataFrame:
    return products.join(translation, "product_category_name", "left").select(
        "product_id",
        "product_category_name",
        "product_category_name_english",
        F.col("product_weight_g").cast("int").alias("product_weight_g"),
        F.col("product_photos_qty").cast("int").alias("product_photos_qty"),
        F.lit(cfg.backfill_date).alias("_delivery_date"),
    )


def build_sellers(spark: SparkSession, sellers: DataFrame, cfg: Config) -> DataFrame:
    """A full seller snapshot every day; ~2% of sellers relocate once during the replay."""
    days = [cfg.backfill_date + dt.timedelta(days=i) for i in range(cfg.replay_days + 1)]
    dates = spark.createDataFrame([(d,) for d in days], "_delivery_date date")
    b = _bucket(cfg.seed, "seller_move", "seller_id")
    move_day = F.date_add(F.lit(cfg.first_replay_date), F.pmod(b, F.lit(cfg.replay_days)).cast("int"))
    target = F.pmod(F.xxhash64(F.lit(cfg.seed), F.col("seller_id")), F.lit(len(RELOCATION_TARGETS)))
    city = F.element_at(F.array(*[F.lit(c) for c, _ in RELOCATION_TARGETS]), (target + 1).cast("int"))
    state = F.element_at(F.array(*[F.lit(s) for _, s in RELOCATION_TARGETS]), (target + 1).cast("int"))

    base = sellers.select("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state")
    # A "move" to the city the seller is already in is not a change; don't count it as one.
    really_moves = (city != F.col("seller_city")) | (state != F.col("seller_state"))
    moved = _in(b, SELLER_MOVE_BUCKET) & really_moves & (F.col("_delivery_date") >= move_day)
    return base.crossJoin(dates).select(
        "seller_id",
        "seller_zip_code_prefix",
        F.when(moved, city).otherwise(F.col("seller_city")).alias("seller_city"),
        F.when(moved, state).otherwise(F.col("seller_state")).alias("seller_state"),
        "_delivery_date",
        # Tag only the first day a seller shows the new address.
        F.when(moved & (F.col("_delivery_date") == move_day), F.lit("seller_relocated")).alias("_anomaly"),
    )


# ---------------------------------------------------------------------------
# anomaly injection
# ---------------------------------------------------------------------------
def inject_item_anomalies(items: DataFrame, cfg: Config) -> DataFrame:
    replay = F.col("_delivery_date") > F.lit(cfg.backfill_date)
    b = _bucket(cfg.seed, "items", "order_id", "order_item_id")
    items = items.withColumn("_b", F.when(replay, b))

    def tagged(name: str) -> Column:
        return _in(F.col("_b"), ITEM_BUCKETS[name])

    late_days = (F.lit(1) + F.pmod(F.col("_b"), F.lit(2))).cast("int")  # 1 or 2 days late
    late_target = F.when(F.date_add("_delivery_date", late_days) <= F.lit(cfg.last_replay_date), F.date_add("_delivery_date", late_days))
    too_late_target = _shift(cfg.late_tolerance_days + 2, cfg)
    poisoned = (F.col("_delivery_date") == F.lit(cfg.poison_date)) & _in(F.col("_b"), POISON_BUCKET) if cfg.poison_date else F.lit(False)

    anomaly = (
        F.when(tagged("late_within_tolerance") & late_target.isNotNull(), "late_within_tolerance")
        .when(tagged("too_late") & too_late_target.isNotNull(), "too_late")
        .when(tagged("invalid_price"), "invalid_price")
        .when(tagged("missing_product_id"), "missing_product_id")
        .when(tagged("unknown_product_id"), "unknown_product_id")
        .when(poisoned, "poison_invalid_price")
    )
    items = items.withColumn("_anomaly", anomaly).select(
        "order_id",
        "order_item_id",
        F.when(F.col("_anomaly") == "missing_product_id", F.lit(None).cast("string"))
        .when(F.col("_anomaly") == "unknown_product_id", F.concat(F.lit("unknown-"), F.col("product_id")))
        .otherwise(F.col("product_id")).alias("product_id"),
        "seller_id",
        "order_purchase_ts",
        "shipping_limit_ts",
        F.when(F.col("_anomaly").isin("invalid_price", "poison_invalid_price"), -F.col("price")).otherwise(F.col("price")).alias("price"),
        "freight_value",
        F.when(F.col("_anomaly") == "late_within_tolerance", late_target)
        .when(F.col("_anomaly") == "too_late", too_late_target)
        .otherwise(F.col("_delivery_date")).alias("_delivery_date"),
        "_anomaly",
        "_b",
    )

    # Duplicates: an extra copy in the same batch, or re-sent the following day.
    same = items.where(tagged("duplicate_same_batch") & F.col("_anomaly").isNull()).withColumn("_anomaly", F.lit("duplicate_same_batch"))
    nxt = (
        items.where(tagged("duplicate_next_batch") & F.col("_anomaly").isNull())
        .withColumn("_delivery_date", _shift(1, cfg))
        .where(F.col("_delivery_date").isNotNull())
        .withColumn("_anomaly", F.lit("duplicate_next_batch"))
    )
    items = items.unionByName(same).unionByName(nxt)

    # Schema evolution: the producer starts sending a new field part-way through the replay.
    evolve_from = cfg.first_replay_date + dt.timedelta(days=NEW_FIELD_AFTER_DAYS)
    discount = F.when(
        F.col("_delivery_date") >= F.lit(evolve_from),
        F.round(F.col("price") * (F.pmod(F.xxhash64(F.col("order_id")), F.lit(6)) / 100), 2).cast(MONEY),
    )
    return items.withColumn(NEW_FIELD, discount).drop("_b")


def inject_order_anomalies(orders: DataFrame, cfg: Config) -> DataFrame:
    replay = F.col("_delivery_date") > F.lit(cfg.backfill_date)
    b = F.when(replay, _bucket(cfg.seed, "orders", "order_id", "change_seq"))
    orders = orders.withColumn("_b", b)
    delayed = _shift(2, cfg)

    anomaly = (
        # An intermediate change that shows up two days after the changes that followed it.
        F.when(_in(F.col("_b"), ORDER_BUCKETS["cdc_out_of_order"]) & delayed.isNotNull(), "cdc_out_of_order")
        .when(_in(F.col("_b"), ORDER_BUCKETS["invalid_delivery_ts"]) & F.col("order_delivered_customer_ts").isNotNull(), "invalid_delivery_ts")
    )
    orders = orders.withColumn("_anomaly", anomaly)
    orders = orders.withColumn(
        "_delivery_date", F.when(F.col("_anomaly") == "cdc_out_of_order", delayed).otherwise(F.col("_delivery_date"))
    ).withColumn(
        "order_delivered_customer_ts",
        F.when(F.col("_anomaly") == "invalid_delivery_ts", F.col("order_purchase_ts") - F.expr("INTERVAL 3 DAYS"))
        .otherwise(F.col("order_delivered_customer_ts")),
    )
    dup = orders.where(_in(F.col("_b"), ORDER_BUCKETS["cdc_duplicate"]) & F.col("_anomaly").isNull()).withColumn(
        "_anomaly", F.lit("cdc_duplicate")
    )
    return orders.unionByName(dup).drop("_b")


# ---------------------------------------------------------------------------
# entry points
# ---------------------------------------------------------------------------
def prepare(spark: SparkSession, cfg: Config) -> None:
    raw = {k: _read_csv(spark, cfg, k) for k in SOURCE_FILES}
    staged = {
        "orders_cdc": inject_order_anomalies(build_orders_cdc(raw["orders"], cfg), cfg),
        "order_items": inject_item_anomalies(build_order_items(raw["items"], raw["orders"], cfg), cfg),
        "customers": build_customers(raw["customers"], raw["orders"], cfg),
        "reviews": build_reviews(raw["reviews"], cfg),
        "sellers": build_sellers(spark, raw["sellers"], cfg),
        "products": build_products(raw["products"], raw["translation"], cfg),
    }

    manifest_parts = []
    for name, df in staged.items():
        if "_anomaly" not in df.columns:
            df = df.withColumn("_anomaly", F.lit(None).cast("string"))
        df = _within_horizon(df, cfg)
        df.write.mode("overwrite").partitionBy("_delivery_date").parquet(f"{cfg.staging_path}/{name}")

        df = spark.read.parquet(f"{cfg.staging_path}/{name}")
        counts = df.groupBy("_delivery_date").agg(F.count("*").alias("row_count"))
        anomalies = (
            df.where(F.col("_anomaly").isNotNull()).groupBy("_delivery_date", "_anomaly").count()
            .groupBy("_delivery_date").agg(F.map_from_entries(F.collect_list(F.struct("_anomaly", "count"))).alias("injected"))
        )
        if NEW_FIELD in df.columns:
            evolved = df.where(F.col(NEW_FIELD).isNotNull()).groupBy("_delivery_date").agg(F.count("*").alias("new_field_rows"))
        else:
            evolved = df.select("_delivery_date").limit(0).withColumn("new_field_rows", F.lit(0).cast("long"))
        manifest_parts.append(
            counts.join(anomalies, "_delivery_date", "left").join(evolved, "_delivery_date", "left").select(
                F.col("_delivery_date").alias("batch_date"),
                F.lit(name).alias("feed"),
                "row_count",
                F.coalesce("injected", F.create_map().cast("map<string,bigint>")).alias("injected"),
                F.coalesce("new_field_rows", F.lit(0).cast("long")).alias("new_field_rows"),
            )
        )

    manifest = manifest_parts[0]
    for part in manifest_parts[1:]:
        manifest = manifest.unionByName(part)
    manifest = manifest.withColumn("delivered_at", F.lit(None).cast("timestamp"))
    manifest.writeTo(cfg.table("ops", "replay_manifest")).using("delta").createOrReplace()


def next_undelivered(spark: SparkSession, cfg: Config) -> dt.date | None:
    row = spark.table(cfg.table("ops", "replay_manifest")).where("delivered_at IS NULL").agg(F.min("batch_date")).first()
    return row[0]


def deliver(spark: SparkSession, cfg: Config, batch_date: dt.date) -> None:
    """Drop one day's files into landing/<feed>/batch_date=YYYY-MM-DD/. Idempotent."""
    for name, feed in FEEDS.items():
        rows = (
            spark.read.parquet(f"{cfg.staging_path}/{name}")
            .where(F.col("_delivery_date") == F.lit(batch_date))
            .drop("_delivery_date", "_anomaly")
            .orderBy(F.rand(cfg.seed))  # upstream gives no ordering guarantee
        )
        if rows.isEmpty():
            continue
        target = f"{cfg.landing_path}/{name}/batch_date={batch_date.isoformat()}"
        writer = rows.coalesce(1).write.mode("overwrite")
        if feed.fmt == "json":
            writer.option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss").json(target)
        else:
            writer.parquet(target)

    spark.sql(
        f"UPDATE {cfg.table('ops', 'replay_manifest')} SET delivered_at = current_timestamp() "
        f"WHERE batch_date = DATE'{batch_date.isoformat()}'"
    )
