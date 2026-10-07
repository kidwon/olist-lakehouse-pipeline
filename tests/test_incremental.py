"""Incremental gold with Change Data Feed: when it may run incrementally, and when it must not (ADR-0011)."""

import uuid

import pytest
from pyspark.sql import functions as F

from olist_pipeline import gold
from olist_pipeline import incremental as inc
from olist_pipeline.config import Config


@pytest.fixture
def cfg(spark):
    """A config whose ops tables live in a fresh schema, so watermarks never leak between tests."""
    schema = f"inc_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE {schema}")
    return Config(base_path="/tmp/unused", run_id="r1", schemas={k: schema for k in ["bronze", "silver", "gold", "ops"]})


def source(spark, cfg, rows):
    name = cfg.table("silver", f"src_{uuid.uuid4().hex[:6]}")
    spark.createDataFrame(rows, "order_id string, v int").write.format("delta").saveAsTable(name)
    return name


def test_first_run_is_full_then_incremental_reads_only_new_changes(spark, cfg):
    src = source(spark, cfg, [("o1", 1), ("o2", 1)])
    target = cfg.table("gold", "fact_x")

    first = inc.plan(spark, cfg, target, [src], full_refresh=False)
    assert first.mode == "full"  # nothing to compare against yet
    spark.createDataFrame([("o1",)], "order_id string").write.format("delta").saveAsTable(target)
    inc.save_watermarks(spark, cfg, target, first.versions)

    spark.sql(f"UPDATE {src} SET v = 2 WHERE order_id = 'o2'")
    spark.sql(f"INSERT INTO {src} VALUES ('o3', 1)")
    second = inc.plan(spark, cfg, target, [src], full_refresh=False)
    assert second.mode == "incremental"
    ids = inc.changed_order_ids(second, {src: lambda ch: ch.select("order_id")})
    assert sorted(r.order_id for r in ids.collect()) == ["o2", "o3"]  # o1 did not change


def test_nothing_new_means_nothing_to_process(spark, cfg):
    src = source(spark, cfg, [("o1", 1)])
    target = cfg.table("gold", "fact_x")
    spark.createDataFrame([("o1",)], "order_id string").write.format("delta").saveAsTable(target)
    inc.ensure_cdf(spark, src)
    inc.save_watermarks(spark, cfg, target, {src: inc.current_version(spark, src)})
    p = inc.plan(spark, cfg, target, [src], full_refresh=False)
    assert p.mode == "incremental" and inc.changed_order_ids(p, {src: lambda ch: ch.select("order_id")}) is None


def test_full_refresh_flag_wins(spark, cfg):
    src = source(spark, cfg, [("o1", 1)])
    target = cfg.table("gold", "fact_x")
    spark.createDataFrame([("o1",)], "order_id string").write.format("delta").saveAsTable(target)
    inc.ensure_cdf(spark, src)
    inc.save_watermarks(spark, cfg, target, {src: inc.current_version(spark, src)})
    assert inc.plan(spark, cfg, target, [src], full_refresh=True).mode == "full"


def test_missing_change_data_falls_back_to_full(spark, cfg):
    """A watermark older than the moment CDF was turned on cannot be trusted: rebuild instead of guessing."""
    src = source(spark, cfg, [("o1", 1)])  # version 0, CDF off
    target = cfg.table("gold", "fact_x")
    spark.createDataFrame([("o1",)], "order_id string").write.format("delta").saveAsTable(target)
    spark.sql(f"INSERT INTO {src} VALUES ('o2', 1)")  # version 1, still without change data
    inc.save_watermarks(spark, cfg, target, {src: 0})
    p = inc.plan(spark, cfg, target, [src], full_refresh=False)  # turns CDF on now (version 2)
    assert p.mode == "full" and "change data unavailable" in p.reason


def test_watermark_moves_only_after_a_successful_merge(spark, cfg):
    """If the rebuild fails, the same changes must be processed again next run, not skipped."""
    src = source(spark, cfg, [("o1", 1)])
    target = cfg.table("gold", "fact_x")
    spark.createDataFrame([("o1",)], "order_id string").write.format("delta").saveAsTable(target)
    inc.ensure_cdf(spark, src)
    before = inc.current_version(spark, src)
    inc.save_watermarks(spark, cfg, target, {src: before})
    spark.sql(f"INSERT INTO {src} VALUES ('o2', 1)")
    spark.createDataFrame([("o1",), ("o2",)], "order_id string").write.format("delta").saveAsTable(cfg.table("silver", "orders"))

    def boom(ids):
        raise RuntimeError("MERGE failed")

    with pytest.raises(RuntimeError):
        gold._run_incremental(spark, cfg, target, {"x": src}, {"x": lambda ch: ch.select("order_id")}, boom)
    assert inc.read_watermarks(spark, cfg, target)[src] == before


def test_stats_record_how_much_was_processed(spark, cfg):
    p = inc.Plan("incremental", "changes since last run", {}, {})
    inc.record_stats(spark, cfg, "gold.fact_x", p, 12, 1000)
    row = spark.table(cfg.table("ops", "gold_incremental_stats")).first()
    assert (row.mode, row.processed_orders, row.total_orders) == ("incremental", 12, 1000)
