"""DQ gate: stop the run before gold is published if too much of this run's input was bad.

The gate reads only `ops.dq_metrics` for the current run_id. It does not roll back silver:
bad rows are already in quarantine and good rows are correct on their own. What it prevents is
publishing numbers built from a batch we do not trust (ADR-0005).
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import Config
from .quality import ALL_RULES


class DataQualityGateError(RuntimeError):
    pass


def evaluate(spark: SparkSession, cfg: Config):
    return (
        spark.table(cfg.table("ops", "dq_metrics"))
        .where((F.col("run_id") == cfg.run_id) & (F.col("rule") == ALL_RULES))
        .groupBy("table_name")
        .agg(F.sum("failed_rows").alias("bad_rows"), F.sum("total_rows").alias("total_rows"))
        .withColumn("bad_ratio", F.col("bad_rows") / F.col("total_rows"))
        .withColumn("threshold", F.lit(cfg.dq_max_bad_ratio))
        .withColumn("passed", F.col("bad_ratio") <= F.col("threshold"))
        .withColumn("run_id", F.lit(cfg.run_id))
        .withColumn("evaluated_at", F.current_timestamp())
    )


def run(spark: SparkSession, cfg: Config) -> None:
    result = evaluate(spark, cfg)
    result.write.format("delta").mode("append").saveAsTable(cfg.table("ops", "dq_gate_log"))
    failed = result.where("NOT passed").collect()
    if failed:
        detail = ", ".join(f"{r.table_name}: {r.bad_rows}/{r.total_rows} ({r.bad_ratio:.1%})" for r in failed)
        raise DataQualityGateError(
            f"DQ gate failed for run {cfg.run_id} (threshold {cfg.dq_max_bad_ratio:.0%}): {detail}. "
            f"Gold was not published; inspect {cfg.table('ops', 'quarantine')}."
        )
