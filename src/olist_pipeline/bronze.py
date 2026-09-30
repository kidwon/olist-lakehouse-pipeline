"""Bronze: land every file exactly once, keep the raw record, parse against the contract.

Incremental file discovery is Auto Loader on Databricks and the plain file-stream source
locally; both run with trigger(availableNow=True) and a checkpoint, so each file is processed
once and a rerun is a no-op.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

from .config import Config
from .contracts import FEEDS, Feed

BATCH_DATE_RE = r"batch_date=(\d{4}-\d{2}-\d{2})"


def parse_json_lines(raw: DataFrame, feed: Feed) -> DataFrame:
    """Parse a `value` column of JSON lines against the feed contract.

    Adds `_rescued_data` holding any field the contract does not know about (as JSON), and
    `_malformed` when the line is not a JSON object at all.
    """
    known = F.array(*[F.lit(f.name) for f in feed.schema.fields])
    as_map = F.from_json(F.col("value"), MapType(StringType(), StringType()))
    extra = F.map_filter(as_map, lambda k, _: ~F.array_contains(known, k))
    parsed = F.from_json(F.col("value"), feed.schema)
    return raw.select(
        parsed.alias("r"),
        F.when(F.size(extra) > 0, F.to_json(extra)).alias("_rescued_data"),
        as_map.isNull().alias("_malformed"),
        F.col("value").alias("_raw"),
        *[c for c in raw.columns if c.startswith("_")],
    ).select("r.*", "_rescued_data", "_malformed", "_raw", *[c for c in raw.columns if c.startswith("_")])


def with_file_metadata(df: DataFrame) -> DataFrame:
    path = F.col("_metadata.file_path")
    return df.select(
        "*",
        path.alias("_source_file"),
        F.to_date(F.regexp_extract(path, BATCH_DATE_RE, 1)).alias("_batch_date"),
        F.current_timestamp().alias("_ingested_at"),
    )


def _read_stream(spark: SparkSession, cfg: Config, feed: Feed) -> DataFrame:
    path = f"{cfg.landing_path}/{feed.name}"
    is_json = feed.fmt == "json"
    if cfg.use_autoloader:
        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "text" if is_json else "parquet")
            .option("cloudFiles.schemaLocation", cfg.checkpoint(f"bronze_{feed.name}_schema"))
        )
    else:
        reader = spark.readStream.format("text" if is_json else "parquet")
    if not is_json:
        reader = reader.schema(feed.schema)
    df = reader.load(path)
    # Partition discovery may add a batch_date column; the file path is the single source of truth.
    df = df.drop("batch_date")
    df = with_file_metadata(df)
    if is_json:
        return parse_json_lines(df, feed)
    return df.withColumn("_rescued_data", F.lit(None).cast("string")).withColumn("_malformed", F.lit(False)).withColumn(
        "_raw", F.lit(None).cast("string")
    )


def ingest(spark: SparkSession, cfg: Config, feed_name: str) -> None:
    feed = FEEDS[feed_name]
    (
        _read_stream(spark, cfg, feed)
        .writeStream.format("delta")
        .option("checkpointLocation", cfg.checkpoint(f"bronze_{feed.name}"))
        .trigger(availableNow=True)
        .toTable(cfg.table("bronze", feed.name))
        .awaitTermination()
    )


def run(spark: SparkSession, cfg: Config) -> None:
    for name in FEEDS:
        ingest(spark, cfg, name)

