"""SCD2 history and the gold joins that depend on it."""

import datetime as dt

from pyspark.sql import functions as F

from olist_pipeline import gold, scd2

D1, D2, D3 = dt.date(2018, 6, 1), dt.date(2018, 6, 2), dt.date(2018, 6, 3)


def snapshot(spark, rows):
    return spark.createDataFrame(rows, "seller_id string, seller_zip_code_prefix string, seller_city string, seller_state string")


def history(spark, name):
    return {(r.seller_id, r.valid_from): r for r in spark.table(name).collect()}


def test_scd2_keeps_history_and_is_idempotent(spark, table_name):
    scd2.apply_snapshot(spark, table_name, snapshot(spark, [("s1", "1", "sao paulo", "SP"), ("s2", "2", "recife", "PE")]), D1)
    # s1 moves; s2 is missing from the snapshot (not a delete)
    moved = snapshot(spark, [("s1", "1", "curitiba", "PR")])
    scd2.apply_snapshot(spark, table_name, moved, D2)
    scd2.apply_snapshot(spark, table_name, moved, D3)  # same content again: no new version

    h = history(spark, table_name)
    assert len(h) == 3
    first = h[("s1", scd2.BEGINNING_OF_TIME)]
    assert (first.seller_city, first.valid_to, first.is_current) == ("sao paulo", D2, False)
    second = h[("s1", D2)]
    assert (second.seller_city, second.valid_to, second.is_current) == ("curitiba", None, True)
    assert h[("s2", scd2.BEGINNING_OF_TIME)].is_current  # absent from a snapshot != deleted


def test_exactly_one_current_version_per_seller(spark, table_name):
    for d, city in [(D1, "a"), (D2, "b"), (D3, "c")]:
        scd2.apply_snapshot(spark, table_name, snapshot(spark, [("s1", "1", city, "SP")]), d)
    t = spark.table(table_name)
    assert t.where("is_current").count() == 1
    assert t.count() == 3


def test_fact_uses_the_seller_version_valid_on_the_order_date(spark, table_name):
    """Revenue for an order placed before a seller moved must stay with the old location."""
    scd2.apply_snapshot(spark, table_name, snapshot(spark, [("s1", "1", "sao paulo", "SP")]), D1)
    scd2.apply_snapshot(spark, table_name, snapshot(spark, [("s1", "1", "curitiba", "PR")]), D3)
    hist = spark.table(table_name)

    items = spark.createDataFrame(
        [("before", 1, "p1", "s1", dt.datetime(2018, 6, 2, 12), 10.0, 1.0, 0), ("after", 1, "p1", "s1", dt.datetime(2018, 6, 3, 1), 20.0, 1.0, 0)],
        "order_id string, order_item_id int, product_id string, seller_id string, order_purchase_ts timestamp, price double, freight_value double, arrival_lag_days int",
    )
    orders = spark.createDataFrame(
        [("before", "c1", "delivered", None, None), ("after", "c2", "shipped", None, None)],
        "order_id string, customer_id string, order_status string, order_delivered_customer_ts timestamp, order_estimated_delivery_ts timestamp",
    )
    customers = spark.createDataFrame([("c1", "u1"), ("c2", "u1")], "customer_id string, customer_unique_id string")

    fact = gold.build_fact_order_item(items, orders, customers, hist)
    state = fact.join(hist.select("seller_sk", "seller_state"), "seller_sk")
    assert {r.order_id: r.seller_state for r in state.collect()} == {"before": "SP", "after": "PR"}
    assert fact.count() == 2  # the point-in-time join must not fan out


def test_item_before_its_order_is_kept_as_unknown(spark):
    items = spark.createDataFrame(
        [("o1", 1, "p1", "s1", dt.datetime(2018, 6, 2), 10.0, 1.0, 0)],
        "order_id string, order_item_id int, product_id string, seller_id string, order_purchase_ts timestamp, price double, freight_value double, arrival_lag_days int",
    )
    empty_orders = spark.createDataFrame([], "order_id string, customer_id string, order_status string, order_delivered_customer_ts timestamp, order_estimated_delivery_ts timestamp")
    empty_customers = spark.createDataFrame([], "customer_id string, customer_unique_id string")
    empty_hist = spark.createDataFrame([], "seller_id string, seller_sk long, valid_from date, valid_to date")
    row = gold.build_fact_order_item(items, empty_orders, empty_customers, empty_hist).first()
    assert row.order_status == "unknown"


def test_dim_customer_counts_people_not_customer_ids(spark):
    """Olist issues a new customer_id per order; a repeat buyer must be one customer with two orders."""
    customers = spark.createDataFrame(
        [("c1", "u1", "1", "sao paulo", "SP"), ("c2", "u1", "2", "recife", "PE"), ("c3", "u2", "3", "natal", "RN")],
        "customer_id string, customer_unique_id string, customer_zip_code_prefix string, customer_city string, customer_state string",
    )
    orders = spark.createDataFrame(
        [("c1", dt.datetime(2018, 1, 1)), ("c2", dt.datetime(2018, 5, 1)), ("c3", dt.datetime(2018, 2, 1))],
        "customer_id string, order_purchase_ts timestamp",
    )
    dim = {r.customer_unique_id: r for r in gold.build_dim_customer(customers, orders).collect()}
    assert len(dim) == 2
    assert dim["u1"].order_count == 2
    assert dim["u1"].customer_city == "recife"  # attributes from the most recent order


def test_mart_excludes_cancelled_orders_from_gmv(spark):
    fact = spark.createDataFrame(
        [
            ("o1", "s1", dt.datetime(2018, 6, 1), 100.0, 10.0, False, True, True),
            ("o2", "s1", dt.datetime(2018, 6, 2), 50.0, 5.0, True, False, None),
            ("o3", "s1", dt.datetime(2018, 6, 3), 30.0, 3.0, False, True, False),
        ],
        "order_id string, seller_id string, order_purchase_ts timestamp, price double, freight_value double, is_cancelled boolean, is_delivered boolean, is_on_time boolean",
    )
    reviews = spark.createDataFrame([("o1", 5), ("o3", 3)], "order_id string, review_score int")
    sellers = spark.createDataFrame([("s1", "x", "SP", True)], "seller_id string, seller_city string, seller_state string, is_current boolean")
    r = gold.build_mart_seller_delivery(fact, reviews, sellers).first()
    assert (r.orders, float(r.gmv), r.delivered_orders, r.on_time_orders) == (2, 130.0, 2, 1)
    assert r.on_time_rate == 0.5
    assert r.avg_review_score == 4.0


def test_dim_date_has_one_row_per_day(spark):
    d = gold.build_dim_date(spark, dt.date(2018, 1, 1), dt.date(2018, 12, 31))
    assert d.count() == 365
    assert d.where(F.col("date_key") == 20180601).first().year_month == "2018-06"
