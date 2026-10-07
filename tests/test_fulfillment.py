"""Accumulating snapshot: one row per order, milestones and stage durations (ADR-0009)."""

import datetime as dt
from decimal import Decimal

from olist_pipeline import gold, silver

T = dt.datetime
CHANGE_SCHEMA = (
    "order_id string, customer_id string, order_status string, order_purchase_ts timestamp, order_approved_ts timestamp, "
    "order_delivered_carrier_ts timestamp, order_delivered_customer_ts timestamp, order_estimated_delivery_ts timestamp, "
    "change_seq int, change_ts timestamp"
)


def changes(spark, rows):
    return spark.createDataFrame(rows, CHANGE_SCHEMA)


def items(spark, rows):
    return spark.createDataFrame(rows, "order_id string, price decimal(12,2)")


def customers(spark, rows):
    return spark.createDataFrame(rows, "customer_id string, customer_unique_id string")


def build(spark, change_rows, item_rows=(), customer_rows=()):
    rows = gold.build_fact_order_fulfillment(changes(spark, change_rows), items(spark, list(item_rows)), customers(spark, list(customer_rows))).collect()
    return {r.order_id: r for r in rows}


P, A, S, D, E = T(2018, 6, 1, 10), T(2018, 6, 1, 12), T(2018, 6, 2, 10), T(2018, 6, 5, 22), T(2018, 6, 5)


def lifecycle(order_id="o1", purchase=P, approved=A, shipped=S, delivered=D, estimated=E):
    """The CDC after-images of one order: each change shows the timestamps known at that point."""
    out = [(order_id, "c1", "created", purchase, None, None, None, estimated, 1, purchase)]
    seq = 1
    for status, ts in [("approved", approved), ("shipped", shipped), ("delivered", delivered)]:
        if ts is None:
            break
        seq += 1
        out.append((order_id, "c1", status, purchase, approved, shipped if seq >= 3 else None,
                    delivered if seq >= 4 else None, estimated, seq, ts))
    return out


def test_one_row_per_order_with_milestones_from_the_latest_change(spark):
    """However many changes an order had, the snapshot has exactly one row carrying all milestones."""
    r = build(spark, lifecycle(), [("o1", Decimal("100")), ("o1", Decimal("20"))], [("c1", "ana")])["o1"]
    assert (r.current_status, r.purchase_ts, r.approved_ts, r.shipped_ts, r.delivered_ts) == ("delivered", P, A, S, D)
    assert (r.item_count, r.order_value, r.customer_unique_id, r.last_change_seq) == (2, Decimal("120.00"), "ana", 4)


def test_stage_durations_are_hours_and_add_up(spark):
    r = build(spark, lifecycle())["o1"]
    assert (r.hours_to_approve, r.hours_to_ship, r.hours_in_transit) == (2.0, 22.0, 84.0)
    assert r.hours_total == r.hours_to_approve + r.hours_to_ship + r.hours_in_transit == 108.0
    assert not r.has_inconsistent_milestones


def test_each_milestone_has_its_own_date_key(spark):
    """Role-playing dim_date: the same date dimension, joined once per milestone."""
    r = build(spark, lifecycle())["o1"]
    assert (r.purchase_date_key, r.approved_date_key, r.shipped_date_key, r.delivered_date_key, r.estimated_date_key) == (
        20180601, 20180601, 20180602, 20180605, 20180605
    )


def test_delivery_any_time_on_the_estimated_day_is_on_time(spark):
    """The estimate is a date, so 22:00 on that day is on time: lateness counts from the end of the day."""
    on_day = build(spark, lifecycle())["o1"]
    assert on_day.is_on_time and on_day.hours_late == -2.0
    next_day = build(spark, lifecycle(delivered=T(2018, 6, 6, 3)))["o1"]
    assert not next_day.is_on_time and next_day.hours_late == 3.0


def test_open_order_has_no_durations_and_is_not_flagged(spark):
    r = build(spark, lifecycle(shipped=None, delivered=None))["o1"]
    assert r.current_status == "approved"
    assert (r.hours_to_ship, r.hours_in_transit, r.hours_total, r.hours_late, r.is_on_time) == (None,) * 5
    # Kimball: never a NULL foreign key; a milestone that has not happened points at the NOT_YET row.
    assert (r.shipped_date_key, r.delivered_date_key) == (gold.NOT_YET, gold.NOT_YET)
    assert not r.has_inconsistent_milestones


def test_cancellation_and_unavailability_end_the_order_with_a_reason(spark):
    cancelled = lifecycle("o1", shipped=None, delivered=None) + [("o1", "c1", "canceled", P, A, None, None, E, 3, T(2018, 6, 1, 13))]
    unavailable = lifecycle("o2", shipped=None, delivered=None) + [("o2", "c1", "unavailable", P, A, None, None, E, 3, T(2018, 6, 3, 9))]
    out = build(spark, cancelled + unavailable)
    assert (out["o1"].end_reason, out["o1"].ended_ts) == ("canceled", T(2018, 6, 1, 13))
    assert (out["o2"].end_reason, out["o2"].ended_ts) == ("unavailable", T(2018, 6, 3, 9))
    assert build(spark, lifecycle())["o1"].end_reason is None


def test_inconsistent_timestamps_are_kept_but_never_turned_into_negative_durations(spark):
    """Real Olist rows hand over to the carrier before the purchase; a negative duration would poison every average."""
    carrier_first = T(2018, 6, 1, 9, 45)  # 15 minutes before the purchase, like most real cases
    r = build(spark, lifecycle(shipped=carrier_first))["o1"]
    assert r.shipped_ts == carrier_first  # the raw value is not altered
    assert r.hours_to_ship is None  # approved 12:00 -> shipped 09:45 would be negative
    assert r.hours_to_approve == 2.0 and r.hours_in_transit is not None
    assert r.has_inconsistent_milestones


def test_metric_counts_flagged_orders_as_a_warning(spark):
    fact = gold.build_fact_order_fulfillment(
        changes(spark, lifecycle("o1") + lifecycle("o2", shipped=T(2018, 6, 1, 9))), items(spark, []), customers(spark, [])
    )
    m = gold.inconsistent_milestone_metric(fact, "r1").first()
    assert (m.rule, m.severity, m.failed_rows, m.total_rows) == ("inconsistent_milestones", "warn", 1, 2)


def test_fact_updates_in_place_as_the_order_moves_forward(spark, table_name):
    """Accumulating snapshot, not a log: a new milestone rewrites the order's row, it never adds one."""
    empty_items, no_customers = items(spark, []), customers(spark, [])
    shipped_so_far = [c for c in lifecycle() if c[8] <= 3]
    gold.merge_fact(spark, table_name, gold.build_fact_order_fulfillment(changes(spark, shipped_so_far), empty_items, no_customers), ["order_id"])
    assert spark.table(table_name).first().current_status == "shipped"
    gold.merge_fact(spark, table_name, gold.build_fact_order_fulfillment(changes(spark, lifecycle()), empty_items, no_customers), ["order_id"])
    rows = spark.table(table_name).collect()
    assert len(rows) == 1 and rows[0].current_status == "delivered" and rows[0].hours_total == 108.0


def test_change_log_keeps_every_change_once(spark, table_name):
    """The log behind the snapshot is append-only: a re-sent change is ignored, nothing is ever updated."""
    log = changes(spark, lifecycle())
    silver.merge_insert_new(spark, table_name, log, ["order_id", "change_seq"])
    silver.merge_insert_new(spark, table_name, log, ["order_id", "change_seq"])
    assert spark.table(table_name).count() == 4


def test_review_score_sits_next_to_the_delay(spark):
    """Delay and satisfaction on the same row: late orders can be compared with how they were rated."""
    reviews = spark.createDataFrame([("o1", 2), ("o1", 3), ("o2", 5)], "order_id string, review_score int")
    fact = gold.build_fact_order_fulfillment(
        changes(spark, lifecycle("o1", delivered=T(2018, 6, 8)) + lifecycle("o2") + lifecycle("o3")),
        items(spark, []), customers(spark, []), reviews,
    )
    out = {r.order_id: r for r in fact.collect()}
    assert (out["o1"].review_score, out["o1"].review_count, out["o1"].is_on_time) == (2.5, 2, False)  # mean of 2 reviews
    assert (out["o2"].review_score, out["o2"].review_count, out["o2"].is_on_time) == (5.0, 1, True)
    assert (out["o3"].review_score, out["o3"].review_count) == (None, 0)  # no review: no score is invented


def test_a_new_column_reaches_an_existing_fact_table(spark, table_name):
    """Deployed tables already exist with the old schema; MERGE must add new columns, not drop them."""
    old = spark.createDataFrame([("o1", 1, "h1")], "order_id string, item_count int, row_hash string")
    gold.merge_fact(spark, table_name, old, ["order_id"])
    # Same hash as the stored row on purpose: a broken earlier deploy can store the new hash while
    # dropping the new column, so a hash comparison alone would never fill it in.
    new = spark.createDataFrame([("o1", 1, 4.0, "h1")], "order_id string, item_count int, review_score double, row_hash string")
    gold.merge_fact(spark, table_name, new, ["order_id"])
    row = spark.table(table_name).first()
    assert "review_score" in spark.table(table_name).columns and row.review_score == 4.0


def test_clearing_row_hash_forces_a_refresh(spark, table_name):
    """The repair path for a table whose rows were stored with a stale hash."""
    gold.merge_fact(spark, table_name, spark.createDataFrame([("o1", None, "h1")], "order_id string, review_score double, row_hash string"), ["order_id"])
    spark.sql(f"UPDATE {table_name} SET row_hash = NULL")
    gold.merge_fact(spark, table_name, spark.createDataFrame([("o1", 4.0, "h1")], "order_id string, review_score double, row_hash string"), ["order_id"])
    assert spark.table(table_name).first().review_score == 4.0
