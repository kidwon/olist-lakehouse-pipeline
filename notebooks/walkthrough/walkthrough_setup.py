"""Shared setup for the walkthrough notebooks.

Makes `src/` importable and returns a Spark session plus a scratch schema, both on Databricks
(the notebooks are synced to the workspace by the bundle) and locally, where each notebook can
also be run as a plain script:  uv run python notebooks/walkthrough/03_silver_dedup_cdc.py
"""

from __future__ import annotations

import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "..", "src")))

ON_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ


def setup():
    """Return (spark, schema). The scratch schema holds only small demo tables."""
    if ON_DATABRICKS:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        schema = "workspace.olist_walkthrough"
    else:
        import time

        # Python datetimes in the demo data mean UTC, as they do on Databricks.
        os.environ["TZ"] = "UTC"
        time.tzset()
        from olist_pipeline.cli import local_spark

        spark = local_spark(os.path.join(tempfile.gettempdir(), "olist_walkthrough"))
        spark.sparkContext.setLogLevel("ERROR")
        schema = "olist_walkthrough"
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    return spark, schema


def fresh_table(spark, schema: str, name: str) -> str:
    """Drop a demo table so every run of a notebook starts from the same state."""
    full = f"{schema}.{name}"
    spark.sql(f"DROP TABLE IF EXISTS {full}")
    return full
