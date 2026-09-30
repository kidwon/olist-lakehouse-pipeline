"""End to end: backfill + 6 daily batches of dirty data, last one poisoned.

The fixture's base data is clean, so every anomaly the pipeline reports must be one the
replay injected, and every injected anomaly must be reported. The replay manifest is the
ground truth; these tests reconcile the pipeline's own metrics against it.
"""

import datetime as dt
from dataclasses import replace

import pytest
from pyspark.sql import Window
from pyspark.sql import functions as F

from olist_fixture import write_fixture
from olist_pipeline import cli
from olist_pipeline.config import Config

BACKFILL = dt.date(2018, 5, 31)
POISON = dt.date(2018, 6, 6)


@pytest.fixture(scope="module")
def run(spark, tmp_path_factory):
    base = tmp_path_factory.mktemp("e2e")
    write_fixture(base / "source")
    cfg = Config(base_path=str(base), backfill_date=BACKFILL, replay_days=6, poison_date=POISON)
    cli.run_local(spark, cfg)

    def count(t):
        return spark.table(cfg.table(*t.split("."))).count()

    before = {t: count(t) for t in ["silver.order_items", "silver.orders", "silver.seller_history"]}
    # A second run with no new files: must change nothing in silver.
    cli.process(spark, replace(cfg, run_id="rerun"))
    after = {t: count(t) for t in before}
    return spark, cfg, before, after


def injected(spark, cfg, feed):
    m = spark.table(cfg.table("ops", "replay_manifest")).where(F.col("feed") == feed)
    rows = m.select(F.explode("injected")).groupBy("key").agg(F.sum("value").alias("n")).collect()
    return {r.key: r.n for r in rows}


def reported(spark, cfg, table_name):
    m = spark.table(cfg.table("ops", "dq_metrics")).where(F.col("table_name") == table_name)
    return {r.rule: r.n for r in m.groupBy("rule").agg(F.sum("failed_rows").alias("n")).collect()}


def test_every_injected_item_anomaly_is_reported(run):
    spark, cfg, *_ = run
    inj, rep = injected(spark, cfg, "order_items"), reported(spark, cfg, "order_items")
    assert inj.get("invalid_price", 0) > 0 and inj.get("poison_invalid_price", 0) > 0
    assert rep["price_not_positive"] == inj["invalid_price"] + inj["poison_invalid_price"]
    assert rep["missing_product_id"] == inj.get("missing_product_id", 0)
    assert rep["unknown_product_id"] == inj.get("unknown_product_id", 0)
    assert rep["too_late"] == inj["too_late"] > 0
    assert rep["duplicate_in_batch"] == inj["duplicate_same_batch"] > 0
    assert rep["duplicate_already_loaded"] == inj["duplicate_next_batch"] > 0


def test_every_invalid_order_change_is_reported(run):
    spark, cfg, *_ = run
    assert reported(spark, cfg, "orders_cdc")["delivered_before_purchase"] == injected(spark, cfg, "orders_cdc")["invalid_delivery_ts"] > 0


def test_new_producer_field_is_rescued_row_for_row(run):
    spark, cfg, *_ = run
    new_rows = spark.table(cfg.table("ops", "replay_manifest")).where("feed = 'order_items'").agg(F.sum("new_field_rows")).first()[0]
    assert new_rows > 0
    assert reported(spark, cfg, "order_items")["rescued_data"] == new_rows
    # ...and nothing was lost: the raw value is still in bronze.
    bronze = spark.table(cfg.table("bronze", "order_items"))
    assert bronze.where(F.col("_rescued_data").contains("discount_amount")).count() == new_rows


def test_silver_items_are_exactly_delivered_minus_duplicates_minus_quarantine(run):
    spark, cfg, *_ = run
    delivered = spark.table(cfg.table("ops", "replay_manifest")).where("feed = 'order_items'").agg(F.sum("row_count")).first()[0]
    inj, rep = injected(spark, cfg, "order_items"), reported(spark, cfg, "order_items")
    items = spark.table(cfg.table("silver", "order_items"))
    expected = delivered - inj["duplicate_same_batch"] - inj["duplicate_next_batch"] - rep["__all__"]
    assert items.count() == expected
    assert items.select("order_id", "order_item_id").distinct().count() == expected


def test_orders_reflect_the_latest_valid_change_despite_out_of_order_arrival(run):
    spark, cfg, *_ = run
    assert injected(spark, cfg, "orders_cdc")["cdc_out_of_order"] > 0
    staged = spark.read.parquet(f"{cfg.staging_path}/orders_cdc").where(
        (F.col("_delivery_date") <= cfg.last_replay_date) & (F.coalesce(F.col("_anomaly"), F.lit("")) != "invalid_delivery_ts")
    )
    w = Window.partitionBy("order_id").orderBy(F.col("change_seq").desc())
    truth = staged.withColumn("rn", F.row_number().over(w)).where("rn = 1").select("order_id", "order_status", "change_seq")
    actual = spark.table(cfg.table("silver", "orders")).select("order_id", "order_status", "change_seq")
    assert truth.exceptAll(actual).count() == 0
    assert actual.exceptAll(truth).count() == 0


def test_late_items_are_accepted_and_dated_by_the_sale(run):
    spark, cfg, *_ = run
    late = spark.table(cfg.table("silver", "order_items")).where(F.col("arrival_lag_days").between(1, cfg.late_tolerance_days))
    assert late.count() == injected(spark, cfg, "order_items")["late_within_tolerance"] > 0
    assert late.where(F.col("event_date") != F.to_date("order_purchase_ts")).count() == 0


def test_gate_blocked_only_the_poisoned_run(run):
    spark, cfg, *_ = run
    log = spark.table(cfg.table("ops", "dq_gate_log")).where("table_name = 'order_items' AND run_id LIKE 'local-%'")
    failed = {r.run_id for r in log.where("NOT passed").collect()}
    assert failed == {f"local-{POISON.isoformat()}"}


def test_seller_relocations_become_closed_scd2_versions(run):
    spark, cfg, *_ = run
    relocated = injected(spark, cfg, "sellers")["seller_relocated"]
    hist = spark.table(cfg.table("silver", "seller_history"))
    assert relocated > 0
    assert hist.where("NOT is_current").count() == relocated
    assert hist.groupBy("seller_id").agg(F.count_if("is_current").alias("c")).where("c != 1").count() == 0


def test_rerun_without_new_files_changes_nothing(run):
    _, _, before, after = run
    assert before == after


def test_gold_fact_matches_silver_after_catch_up_run(run):
    """The poisoned day was held back; the next clean run publishes everything silver accepted."""
    spark, cfg, *_ = run
    fact = spark.table(cfg.table("gold", "fact_order_item"))
    assert fact.count() == spark.table(cfg.table("silver", "order_items")).count()
    assert fact.where(F.col("order_date_key") == int(POISON.strftime("%Y%m%d"))).count() > 0
    assert fact.where(F.col("seller_sk").isNull()).count() == 0
    mart = spark.table(cfg.table("gold", "mart_seller_delivery_performance"))
    assert mart.agg(F.sum("items")).first()[0] == fact.where("NOT is_cancelled").count()
