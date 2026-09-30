"""Command-line entry point. One sub-command per job task.

Databricks (wheel task):  olist <task> --env databricks --run-id {{job.run_id}}
Local:                     olist <task> --base-path ./data
Local, everything:         olist run-local --base-path ./data
"""

from __future__ import annotations

import argparse
import datetime as dt
import uuid

from pyspark.sql import SparkSession

from . import bronze, gate, gold, replay, scd2, silver
from .config import Config

DATABRICKS_CATALOG = "workspace"
DATABRICKS_BASE = "/Volumes/workspace/olist_raw/files"


def local_spark(base_path: str) -> SparkSession:
    import os
    import sys

    from delta import configure_spark_with_delta_pip

    # Python workers must use the same interpreter as the driver.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("olist-pipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", f"{base_path}/_warehouse")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={base_path}/_derby")
        .enableHiveSupport()
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def get_spark(cfg: Config) -> SparkSession:
    spark = SparkSession.builder.getOrCreate() if cfg.catalog else local_spark(cfg.base_path)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def setup(spark: SparkSession, cfg: Config) -> None:
    if cfg.catalog:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.olist_raw")
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.olist_raw.files")
    for layer in cfg.schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema_name(layer)}")


def replay_next(spark: SparkSession, cfg: Config, batch_date: str) -> dt.date | None:
    day = replay.next_undelivered(spark, cfg) if batch_date == "next" else dt.date.fromisoformat(batch_date)
    if day is None:
        print("Replay finished: every planned batch has been delivered.")
        return None
    replay.deliver(spark, cfg, day)
    print(f"Delivered batch {day}")
    return day


def process(spark: SparkSession, cfg: Config) -> None:
    """Everything after the file drop, in DAG order."""
    bronze.run(spark, cfg)
    silver.run(spark, cfg)
    scd2.run(spark, cfg)
    gate.run(spark, cfg)
    gold.run(spark, cfg)
    gold.run_mart(spark, cfg)


def build_config(args: argparse.Namespace) -> Config:
    on_databricks = args.env == "databricks"
    kwargs = dict(
        base_path=args.base_path or (DATABRICKS_BASE if on_databricks else "./data"),
        catalog=DATABRICKS_CATALOG if on_databricks else None,
        use_autoloader=on_databricks,
        run_id=args.run_id or uuid.uuid4().hex[:12],
        replay_days=args.replay_days,
    )
    if args.backfill_date:
        kwargs["backfill_date"] = dt.date.fromisoformat(args.backfill_date)
    if args.poison_date:
        kwargs["poison_date"] = dt.date.fromisoformat(args.poison_date)
    return Config(**kwargs)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="olist")
    p.add_argument(
        "task",
        choices=["setup", "prepare", "replay", "bronze", "silver", "scd2", "gate", "gold", "mart", "run-local"],
    )
    p.add_argument("--env", choices=["local", "databricks"], default="local")
    p.add_argument("--base-path")
    p.add_argument("--run-id")
    p.add_argument("--batch-date", default="next", help="'next' or YYYY-MM-DD")
    p.add_argument("--backfill-date")
    p.add_argument("--replay-days", type=int, default=10)
    p.add_argument("--poison-date")
    args = p.parse_args(argv)

    cfg = build_config(args)
    spark = get_spark(cfg)
    tasks = {
        "setup": lambda: setup(spark, cfg),
        "prepare": lambda: replay.prepare(spark, cfg),
        "replay": lambda: replay_next(spark, cfg, args.batch_date),
        "bronze": lambda: bronze.run(spark, cfg),
        "silver": lambda: silver.run(spark, cfg),
        "scd2": lambda: scd2.run(spark, cfg),
        "gate": lambda: gate.run(spark, cfg),
        "gold": lambda: gold.run(spark, cfg),
        "mart": lambda: gold.run_mart(spark, cfg),
        "run-local": lambda: run_local(spark, cfg),
    }
    tasks[args.task]()


def run_local(spark: SparkSession, cfg: Config) -> None:
    """Setup, prepare, then one full pipeline run per replay day (each with its own run_id)."""
    from dataclasses import replace

    setup(spark, cfg)
    replay.prepare(spark, cfg)
    while (day := replay_next(spark, cfg, "next")) is not None:
        run_cfg = replace(cfg, run_id=f"local-{day.isoformat()}")
        try:
            process(spark, run_cfg)
        except gate.DataQualityGateError as e:
            print(f"[{day}] {e}")


if __name__ == "__main__":
    main()
