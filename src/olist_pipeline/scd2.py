"""SCD Type 2 seller history from daily full snapshots, written by hand with one MERGE (ADR-0003).

For each snapshot (processed in date order):
  * a seller we have never seen gets its first version, valid from 1900-01-01;
  * a seller whose tracked attributes changed gets its current version closed
    (valid_to = snapshot date) and a new current version opened;
  * an unchanged seller is left alone, so re-processing a snapshot is a no-op;
  * a seller missing from a snapshot is kept current (a snapshot is not a delete signal).
"""

from __future__ import annotations

import datetime as dt

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import quality as dq
from .config import Config
from .contracts import SELLERS

KEY = "seller_id"
TRACKED = ["seller_zip_code_prefix", "seller_city", "seller_state"]
BEGINNING_OF_TIME = dt.date(1900, 1, 1)
HISTORY_COLS = [
    "seller_sk", KEY, *TRACKED, "attr_hash", "valid_from", "valid_to", "is_current",
]


def attr_hash() -> F.Column:
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("<null>")) for c in TRACKED]), 256)


def stage_changes(current: DataFrame, snapshot: DataFrame, snapshot_date: dt.date) -> DataFrame:
    """Build the MERGE source.

    Rows with `merge_key` = seller_id close the matching current version; rows with a NULL
    `merge_key` never match and are inserted as new current versions.
    """
    snap = snapshot.select(KEY, *TRACKED).withColumn("attr_hash", attr_hash())
    cur = current.where("is_current").select(KEY, F.col("attr_hash").alias("cur_hash"))

    changed = snap.join(cur, KEY).where(F.col("attr_hash") != F.col("cur_hash")).drop("cur_hash")
    new = snap.join(cur, KEY, "left_anti")

    def version(df: DataFrame, valid_from: dt.date) -> DataFrame:
        return df.withColumn("valid_from", F.lit(valid_from))

    inserts = version(changed, snapshot_date).unionByName(version(new, BEGINNING_OF_TIME))
    inserts = inserts.withColumn("merge_key", F.lit(None).cast("string"))
    closes = version(changed, snapshot_date).withColumn("merge_key", F.col(KEY))
    return closes.unionByName(inserts).withColumn("seller_sk", F.xxhash64(KEY, "valid_from"))


def apply_snapshot(spark: SparkSession, target_name: str, snapshot: DataFrame, snapshot_date: dt.date) -> None:
    if not spark.catalog.tableExists(target_name):
        empty = snapshot.select(KEY, *TRACKED).limit(0).select(
            F.lit(None).cast("long").alias("seller_sk"), KEY, *TRACKED,
            F.lit(None).cast("string").alias("attr_hash"),
            F.lit(None).cast("date").alias("valid_from"),
            F.lit(None).cast("date").alias("valid_to"),
            F.lit(None).cast("boolean").alias("is_current"),
        )
        empty.write.format("delta").saveAsTable(target_name)
    target = DeltaTable.forName(spark, target_name)
    staged = stage_changes(target.toDF(), snapshot, snapshot_date)
    (
        target.alias("t")
        .merge(staged.alias("s"), f"t.{KEY} = s.merge_key AND t.is_current")
        .whenMatchedUpdate(set={"valid_to": "s.valid_from", "is_current": "false"})
        .whenNotMatchedInsert(values={
            "seller_sk": "s.seller_sk",
            KEY: f"s.{KEY}",
            **{c: f"s.{c}" for c in TRACKED},
            "attr_hash": "s.attr_hash",
            "valid_from": "s.valid_from",
            "valid_to": "CAST(NULL AS DATE)",
            "is_current": "true",
        })
        .execute()
    )


def run(spark: SparkSession, cfg: Config) -> None:
    target_name = cfg.table("silver", "seller_history")
    rules = dq.snapshot_rules(KEY)
    cols = [f.name for f in SELLERS.schema.fields]

    def fn(mb: DataFrame, _: int) -> None:
        s = mb.sparkSession
        good = dq.record(s, cfg, dq.evaluate(mb, rules), rules, "sellers", cols)
        # Several daily snapshots can arrive in one micro-batch (e.g. after an outage): apply them in order.
        dates = sorted(r[0] for r in good.select("_batch_date").distinct().collect())
        for d in dates:
            apply_snapshot(s, target_name, good.where(F.col("_batch_date") == F.lit(d)), d)

    (
        spark.readStream.table(cfg.table("bronze", "sellers"))
        .writeStream.foreachBatch(fn)
        .option("checkpointLocation", cfg.checkpoint("silver_seller_history"))
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )
