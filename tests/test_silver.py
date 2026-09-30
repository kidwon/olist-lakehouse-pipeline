"""Silver behaviour that protects the numbers downstream: no double counting, no status regression."""

import datetime as dt

from pyspark.sql import functions as F

from olist_pipeline import quality as dq
from olist_pipeline import silver
from olist_pipeline.bronze import parse_json_lines
from olist_pipeline.config import Config
from olist_pipeline.contracts import ORDER_ITEMS

T0 = dt.datetime(2018, 6, 1, 10, 0)


def orders_df(spark, rows):
    return spark.createDataFrame(rows, "order_id string, order_status string, change_seq int, _ingested_at timestamp")


# --- de-duplication ---------------------------------------------------------
def test_redelivered_item_is_not_counted_twice(spark, table_name):
    """The same line re-sent in a later batch must not double revenue."""
    schema = "order_id string, order_item_id int, price double"
    silver.merge_insert_new(spark, table_name, spark.createDataFrame([("o1", 1, 10.0), ("o1", 2, 5.0)], schema), ["order_id", "order_item_id"])
    silver.merge_insert_new(spark, table_name, spark.createDataFrame([("o1", 1, 10.0), ("o2", 1, 7.0)], schema), ["order_id", "order_item_id"])

    t = spark.table(table_name)
    assert t.count() == 3
    assert t.agg(F.sum("price")).first()[0] == 22.0


def test_keep_first_removes_in_batch_duplicates(spark):
    df = spark.createDataFrame([("o1", 1, "a"), ("o1", 1, "b"), ("o1", 2, "c")], "order_id string, order_item_id int, f string")
    out = silver.keep_first(df, ["order_id", "order_item_id"], [F.col("f")])
    assert sorted((r.order_item_id, r.f) for r in out.collect()) == [(1, "a"), (2, "c")]


# --- CDC --------------------------------------------------------------------
def test_latest_change_wins_regardless_of_arrival_order(spark):
    df = orders_df(spark, [("o1", "delivered", 4, T0), ("o1", "created", 1, T0), ("o1", "shipped", 3, T0)])
    out = silver.latest_change_per_order(df).collect()
    assert [(r.order_status, r.change_seq) for r in out] == [("delivered", 4)]


def test_late_older_change_does_not_regress_status(spark, table_name):
    """A 'shipped' change arriving after 'delivered' must not un-deliver the order."""
    silver.merge_cdc_forward_only(spark, table_name, orders_df(spark, [("o1", "delivered", 4, T0)]))
    silver.merge_cdc_forward_only(spark, table_name, orders_df(spark, [("o1", "shipped", 3, T0)]))
    silver.merge_cdc_forward_only(spark, table_name, orders_df(spark, [("o1", "delivered", 4, T0)]))  # duplicate

    assert [(r.order_status, r.change_seq) for r in spark.table(table_name).collect()] == [("delivered", 4)]


def test_newer_change_moves_order_forward(spark, table_name):
    silver.merge_cdc_forward_only(spark, table_name, orders_df(spark, [("o1", "approved", 2, T0)]))
    silver.merge_cdc_forward_only(spark, table_name, orders_df(spark, [("o1", "canceled", 3, T0)]))
    assert spark.table(table_name).first().order_status == "canceled"


# --- data quality -------------------------------------------------------------
def _items(spark, rows):
    schema = "order_id string, order_item_id int, product_id string, price double, freight_value double, " \
             "order_purchase_ts timestamp, _batch_date date, _malformed boolean, _product_known boolean"
    return spark.createDataFrame(rows, schema)


def test_each_bad_row_carries_every_rule_it_breaks(spark):
    cfg = Config(base_path="/tmp", backfill_date=dt.date(2018, 5, 31))
    d = dt.date(2018, 6, 10)
    rows = [
        ("ok", 1, "p1", 10.0, 1.0, T0.replace(day=9), d, False, True),
        ("neg", 1, "p1", -1.0, 1.0, T0.replace(day=9), d, False, True),
        ("both", 1, None, 0.0, 1.0, T0.replace(day=9), d, False, False),
        ("unknown", 1, "zzz", 10.0, 1.0, T0.replace(day=9), d, False, False),
        ("late4", 1, "p1", 10.0, 1.0, T0.replace(day=6), d, False, True),
        ("late3", 1, "p1", 10.0, 1.0, T0.replace(day=7), d, False, True),  # exactly at tolerance: allowed
    ]
    out = {r.order_id: r._failed_rules for r in dq.evaluate(_items(spark, rows), dq.item_rules(cfg)).collect()}
    assert out["ok"] == []
    assert out["neg"] == ["price_not_positive"]
    assert sorted(out["both"]) == ["missing_product_id", "price_not_positive"]  # not also unknown_product_id
    assert out["unknown"] == ["unknown_product_id"]
    assert out["late4"] == ["too_late"]
    assert out["late3"] == []


def test_backfill_history_is_not_flagged_late(spark):
    """The backfill batch carries years of history on purpose; lateness only applies to daily batches."""
    cfg = Config(base_path="/tmp", backfill_date=dt.date(2018, 5, 31))
    rows = [("old", 1, "p1", 10.0, 1.0, dt.datetime(2017, 1, 1), cfg.backfill_date, False, True)]
    assert dq.evaluate(_items(spark, rows), dq.item_rules(cfg)).first()._failed_rules == []


def test_metrics_count_bad_rows_once_and_each_rule_separately(spark):
    cfg = Config(base_path="/tmp")
    d = dt.date(2018, 6, 10)
    rows = [
        ("a", 1, None, -1.0, 1.0, T0, d, False, False),
        ("b", 1, "p1", 10.0, 1.0, T0.replace(day=9), d, False, True),
    ]
    ev = dq.evaluate(_items(spark, rows), dq.item_rules(cfg))
    m = {r.rule: (r.failed_rows, r.total_rows) for r in dq.metrics(ev, dq.item_rules(cfg), "order_items", "r1").collect()}
    assert m[dq.ALL_RULES] == (1, 2)  # one bad row, even though it broke two rules
    assert m["price_not_positive"] == (1, 2)
    assert m["missing_product_id"] == (1, 2)


# --- contract parsing -----------------------------------------------------------
def test_unknown_fields_are_rescued_not_dropped(spark):
    """A producer adding a field must not silently lose data or change our schema."""
    raw = spark.createDataFrame(
        [('{"order_id":"o1","order_item_id":1,"price":9.5,"discount_amount":0.5}',), ("not json",)], "value string"
    )
    rows = parse_json_lines(raw, ORDER_ITEMS).collect()
    good, broken = rows
    assert good.order_id == "o1" and float(good.price) == 9.5
    assert good._rescued_data == '{"discount_amount":"0.5"}'
    assert "discount_amount" not in good.asDict()
    assert broken._malformed is True
