"""Incremental gold with Delta Change Data Feed (ADR-0011).

A gold fact is rebuilt only for the orders whose inputs changed since the last successful run.
Per (target fact, source table) the last processed source version is kept in
`ops.gold_watermarks`; it moves only after the fact's MERGE succeeded, so a failed run is simply
processed again next time. Whenever the incremental path cannot be trusted (first run, no
watermark, change data no longer available, or an explicit full refresh), the fact is rebuilt in
full instead, and the reason is recorded.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Config

CDF_PROPERTY = "delta.enableChangeDataFeed"


def ensure_cdf(spark: SparkSession, table: str) -> None:
    """Turn Change Data Feed on for an existing table (idempotent).

    Changes are only recorded from this point on, which is why a source without a watermark
    always triggers a full rebuild first.
    """
    props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    if props.get(CDF_PROPERTY, "false").lower() != "true":
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ({CDF_PROPERTY} = true)")


def current_version(spark: SparkSession, table: str) -> int:
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]


def changes_between(spark: SparkSession, table: str, after_version: int, up_to_version: int) -> DataFrame:
    """Rows changed in versions (after_version, up_to_version]: inserts and both images of updates."""
    return (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", after_version + 1)
        .option("endingVersion", up_to_version)
        .table(table)
    )


# ---------------------------------------------------------------------------
# watermarks
# ---------------------------------------------------------------------------
def read_watermarks(spark: SparkSession, cfg: Config, target: str) -> dict[str, int]:
    name = cfg.table("ops", "gold_watermarks")
    if not spark.catalog.tableExists(name):
        return {}
    rows = spark.table(name).where(F.col("target") == target).collect()
    return {r["source"]: r["version"] for r in rows}


def save_watermarks(spark: SparkSession, cfg: Config, target: str, versions: dict[str, int]) -> None:
    """Called only after the target's MERGE succeeded."""
    name = cfg.table("ops", "gold_watermarks")
    rows = spark.createDataFrame(
        [(target, src, int(v), cfg.run_id) for src, v in versions.items()],
        "target string, source string, version long, run_id string",
    ).withColumn("updated_at", F.current_timestamp())
    if not spark.catalog.tableExists(name):
        rows.limit(0).write.format("delta").saveAsTable(name)
    from delta.tables import DeltaTable

    (
        DeltaTable.forName(spark, name).alias("t")
        .merge(rows.alias("s"), "t.target = s.target AND t.source = s.source")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


# ---------------------------------------------------------------------------
# planning
# ---------------------------------------------------------------------------
@dataclass
class Plan:
    """What to do for one fact this run."""

    mode: str  # "full" or "incremental"
    reason: str
    versions: dict[str, int]  # source table -> version this run reads up to
    changes: dict[str, DataFrame]  # source table -> changed rows (incremental only)


def plan(spark: SparkSession, cfg: Config, target: str, sources: list[str], full_refresh: bool) -> Plan:
    for s in sources:
        ensure_cdf(spark, s)
    # Fix the upper bound first: anything committed while we work is picked up next run.
    versions = {s: current_version(spark, s) for s in sources}
    if full_refresh:
        return Plan("full", "full refresh requested", versions, {})
    if not spark.catalog.tableExists(target):
        return Plan("full", "target does not exist yet", versions, {})
    marks = read_watermarks(spark, cfg, target)
    missing = [s for s in sources if s not in marks]
    if missing:
        return Plan("full", f"no watermark for {', '.join(sorted(missing))}", versions, {})
    changes = {}
    for s in sources:
        if versions[s] <= marks[s]:
            changes[s] = None  # nothing new in this source
            continue
        try:
            df = changes_between(spark, s, marks[s], versions[s])
            df.limit(1).collect()  # fail now, not mid-MERGE, if the versions are gone
        except Exception as e:  # noqa: BLE001 - any failure here means "can't trust incremental"
            return Plan("full", f"change data unavailable for {s}: {type(e).__name__}", versions, {})
        changes[s] = df
    return Plan("incremental", "changes since last run", versions, changes)


def changed_order_ids(plan_: Plan, mapping: dict[str, callable]) -> DataFrame | None:
    """Union the order ids touched in each source; `mapping` turns a source's changes into order ids."""
    parts = [fn(plan_.changes[s]).select("order_id") for s, fn in mapping.items() if plan_.changes.get(s) is not None]
    if not parts:
        return None
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out.where(F.col("order_id").isNotNull()).distinct()


def record_stats(spark: SparkSession, cfg: Config, target: str, plan_: Plan, processed_orders: int, total_orders: int) -> None:
    row = spark.createDataFrame(
        [(cfg.run_id, target, plan_.mode, plan_.reason, int(processed_orders), int(total_orders))],
        "run_id string, target string, mode string, reason string, processed_orders long, total_orders long",
    ).withColumn("measured_at", F.current_timestamp())
    row.write.format("delta").mode("append").saveAsTable(cfg.table("ops", "gold_incremental_stats"))
