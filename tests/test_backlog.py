"""Periodic snapshot: open orders per day x status x customer state, by event time (ADR-0010)."""

import datetime as dt
from decimal import Decimal

from pyspark.sql import functions as F

from olist_pipeline import gold

T = dt.datetime
D = dt.date
CHANGE_SCHEMA = (
    "order_id string, customer_id string, order_status string, order_purchase_ts timestamp, "
    "order_estimated_delivery_ts timestamp, change_seq int, change_ts timestamp"
)


def change(order_id, status, seq, ts, purchase=T(2018, 6, 1, 10), estimated=T(2018, 6, 4), customer="c1"):
    return (order_id, customer, status, purchase, estimated, seq, ts)


def build(spark, rows, items=(("o1", Decimal("100")),), customers=(("c1", "SP"),)):
    changes = spark.createDataFrame(rows, CHANGE_SCHEMA)
    item_df = spark.createDataFrame(list(items), "order_id string, price decimal(12,2)")
    cust_df = spark.createDataFrame(list(customers), "customer_id string, customer_state string")
    return gold.build_fact_daily_order_backlog(changes, item_df, cust_df)


def by_day(df, order_status=None):
    rows = df.where(F.col("order_status") == order_status) if order_status else df
    return {r.snapshot_date: r for r in rows.collect()}


LIFECYCLE = [
    change("o1", "created", 1, T(2018, 6, 1, 10)),
    change("o1", "approved", 2, T(2018, 6, 1, 11)),
    change("o1", "shipped", 3, T(2018, 6, 3, 9)),
    change("o1", "delivered", 4, T(2018, 6, 6, 15)),
    change("o2", "created", 1, T(2018, 6, 7, 8), purchase=T(2018, 6, 7, 8)),  # keeps the log open until 06-07
]


def test_an_order_is_counted_once_a_day_under_the_status_it_had_at_the_end_of_that_day(spark):
    out = build(spark, LIFECYCLE)
    o1 = out.where("snapshot_date <= DATE'2018-06-06'")
    # created and approved both happen on 06-01: at the end of the day it is approved.
    assert {(r.snapshot_date, r.order_status) for r in o1.collect()} == {
        (D(2018, 6, 1), "approved"), (D(2018, 6, 2), "approved"),
        (D(2018, 6, 3), "shipped"), (D(2018, 6, 4), "shipped"), (D(2018, 6, 5), "shipped"),
    }
    assert o1.agg(F.sum("open_orders")).first()[0] == 5  # one order, five open days, never twice on one day


def test_a_delivered_or_cancelled_order_leaves_the_backlog(spark):
    rows = LIFECYCLE[:2] + [change("o1", "canceled", 3, T(2018, 6, 2, 12)), LIFECYCLE[4]]
    days = {r.snapshot_date for r in build(spark, rows).where("customer_state = 'SP' AND order_status <> 'created'").collect()}
    assert days == {D(2018, 6, 1)}  # approved on 06-01, cancelled on 06-02


def test_measures_age_overdue_and_value(spark):
    out = by_day(build(spark, LIFECYCLE), "shipped")
    assert out[D(2018, 6, 4)].overdue_orders == 0  # estimated date itself is not overdue
    assert out[D(2018, 6, 5)].overdue_orders == 1
    assert out[D(2018, 6, 5)].median_age_days == 4
    assert out[D(2018, 6, 5)].open_order_value == Decimal("100.00")


def test_a_stuck_order_stays_visible_and_counts_as_open_over_30_days(spark):
    """Real Olist orders stop in 'processing' forever; they must stay in the backlog, not vanish."""
    stuck = [
        change("o1", "created", 1, T(2018, 5, 1, 10), purchase=T(2018, 5, 1, 10)),
        change("o1", "processing", 2, T(2018, 5, 1, 12), purchase=T(2018, 5, 1, 10)),
        change("o2", "created", 1, T(2018, 6, 10, 8), purchase=T(2018, 6, 10, 8)),
    ]
    last = by_day(build(spark, stuck), "processing")[D(2018, 6, 10)]
    assert (last.open_orders, last.median_age_days, last.orders_open_over_30_days) == (1, 40, 1)


def test_a_change_dated_before_the_purchase_never_counts_the_order_before_it_existed(spark):
    """Real Olist rows are handed to the carrier months before the purchase."""
    p = T(2018, 6, 5, 10)
    rows = [
        change("o1", "shipped", 1, T(2018, 1, 26, 13), purchase=p, estimated=T(2018, 6, 20)),
        change("o1", "created", 2, p, purchase=p, estimated=T(2018, 6, 20)),
        change("o2", "created", 1, T(2018, 6, 7, 8), purchase=T(2018, 6, 7, 8)),
    ]
    first_day = build(spark, rows).where("customer_state = 'SP'").agg(F.min("snapshot_date")).first()[0]
    assert first_day == D(2018, 6, 5)


def test_missing_customer_state_becomes_unknown_never_null(spark):
    """A NULL merge key never matches, so the row would be duplicated on every run."""
    out = build(spark, LIFECYCLE, customers=[])
    assert out.where("customer_state IS NULL").count() == 0
    assert out.where("customer_state = 'unknown'").count() > 0


def test_a_late_change_restates_past_days_and_removes_combinations(spark, table_name):
    """By event time: the shipped change arrives late, so days already published are corrected."""
    without_shipped = [r for r in LIFECYCLE if r[2] != "shipped"]
    gold.merge_fact(spark, table_name, build(spark, without_shipped), gold.BACKLOG_KEYS, delete_missing=True)
    before = by_day(spark.table(table_name).where("customer_state = 'SP'"))
    assert {d: r.order_status for d, r in before.items()}[D(2018, 6, 4)] == "approved"

    gold.merge_fact(spark, table_name, build(spark, LIFECYCLE), gold.BACKLOG_KEYS, delete_missing=True)
    after = spark.table(table_name).where("customer_state = 'SP'")
    assert {(r.snapshot_date, r.order_status) for r in after.collect()} >= {(D(2018, 6, 4), "shipped")}
    # The "approved" rows of 06-03..06-05 no longer exist and were deleted, not left behind.
    assert after.where("order_status = 'approved' AND snapshot_date > DATE'2018-06-02'").count() == 0
    m = spark.sql(f"DESCRIBE HISTORY {table_name}").where("operation = 'MERGE'").orderBy(F.desc("version")).first().operationMetrics
    assert int(m["numTargetRowsDeleted"]) == 3 and int(m["numTargetRowsInserted"]) == 3
