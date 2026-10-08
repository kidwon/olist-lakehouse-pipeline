"""Seller SCD Type 2 from the daily snapshots, declaratively (ADR-0012, compare ADR-0003).

The SQL form of AUTO CDC FROM SNAPSHOT snapshots a table as it is *now*. The landing zone holds
one full snapshot per day, so they must be replayed in date order, which needs the Python API:
the function below hands the pipeline the next snapshot after the last one it processed.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("olist.landing")  # noqa: F821 - `spark` is provided by the pipeline runtime
SELLER_COLUMNS = ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"]


def _snapshots():
    # The file path holds the delivery day (landing/sellers/batch_date=YYYY-MM-DD/...).
    return (
        spark.read.parquet(f"{LANDING}/sellers")  # noqa: F821
        .withColumn("_version", F.date_format(F.to_date(F.regexp_extract(F.col("_metadata.file_path"), r"batch_date=([0-9-]{10})", 1)), "yyyyMMdd").cast("int"))
    )


def next_snapshot_and_version(latest_snapshot_version):
    """Return (snapshot, version) for the first day after `latest_snapshot_version`, or None when done."""
    all_days = _snapshots()
    pending = all_days if latest_snapshot_version is None else all_days.where(F.col("_version") > latest_snapshot_version)
    nxt = pending.agg(F.min("_version")).first()[0]
    if nxt is None:
        return None
    return all_days.where(F.col("_version") == nxt).select(*SELLER_COLUMNS), nxt


dp.create_streaming_table(name="silver_seller_history")

dp.create_auto_cdc_from_snapshot_flow(
    target="silver_seller_history",
    source=next_snapshot_and_version,
    keys=["seller_id"],
    stored_as_scd_type=2,
)
