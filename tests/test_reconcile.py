"""The daily reconciliation is the declarative pipeline's only test, so it must not miss a difference."""

from olist_pipeline import reconcile

SCHEMA = "order_id string, price double"


def test_identical_tables_reconcile(spark):
    a = spark.createDataFrame([("o1", 10.0), ("o2", 5.0)], SCHEMA)
    b = spark.createDataFrame([("o2", 5.0), ("o1", 10.0)], SCHEMA)
    assert reconcile.difference(a, b) == (0, 0)


def test_a_changed_value_is_reported_on_both_sides(spark):
    a = spark.createDataFrame([("o1", 10.0), ("o2", 5.0)], SCHEMA)
    b = spark.createDataFrame([("o1", 10.0), ("o2", 6.0)], SCHEMA)
    assert reconcile.difference(a, b) == (1, 1)


def test_a_duplicated_row_is_a_difference(spark):
    """Double counting is exactly what the business tables must never do, so duplicates count."""
    a = spark.createDataFrame([("o1", 10.0)], SCHEMA)
    b = spark.createDataFrame([("o1", 10.0), ("o1", 10.0)], SCHEMA)
    assert reconcile.difference(a, b) == (0, 1)


def test_ops_tables_compare_as_sets(spark):
    """The imperative ops tables append again when a batch is reprocessed; that is not a difference."""
    a = spark.createDataFrame([("o1", 10.0), ("o1", 10.0)], SCHEMA)
    b = spark.createDataFrame([("o1", 10.0)], SCHEMA)
    assert reconcile.difference(a, b, as_set=True) == (0, 0)
