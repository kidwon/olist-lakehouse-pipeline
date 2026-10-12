"""Seller SCD Type 2 from the daily snapshots, declaratively (ADR-0012, compare ADR-0003).

The SQL form of AUTO CDC FROM SNAPSHOT snapshots a table as it is *now*. The landing zone holds
one full snapshot per day, so they must be replayed in date order, which needs the Python API:
the function below hands the pipeline the next snapshot after the last one it processed.
"""

import os
import re

from pyspark import pipelines as dp

LANDING = spark.conf.get("olist.landing")  # noqa: F821 - `spark` is provided by the pipeline runtime
SELLERS = f"{LANDING}/sellers"
SELLER_COLUMNS = ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"]


def _snapshot_days():
    """Delivery days as {version: folder}, version = int yyyyMMdd (landing/sellers/batch_date=YYYY-MM-DD/).

    Listed from the file system, not with a Spark action: pipeline code should only define
    DataFrames and leave running them to the pipeline.
    """
    days = {}
    for name in os.listdir(SELLERS):
        m = re.fullmatch(r"batch_date=(\d{4})-(\d{2})-(\d{2})", name)
        if m:
            days[int("".join(m.groups()))] = name
    return days


def next_snapshot_and_version(latest_snapshot_version):
    """Return (snapshot, version) for the first day after `latest_snapshot_version`, or None when done."""
    days = _snapshot_days()
    pending = [v for v in days if latest_snapshot_version is None or v > latest_snapshot_version]
    if not pending:
        return None
    nxt = min(pending)
    return spark.read.parquet(f"{SELLERS}/{days[nxt]}").select(*SELLER_COLUMNS), nxt  # noqa: F821


dp.create_streaming_table(name="silver_seller_history")

dp.create_auto_cdc_from_snapshot_flow(
    target="silver_seller_history",
    source=next_snapshot_and_version,
    keys=["seller_id"],
    stored_as_scd_type=2,
)
