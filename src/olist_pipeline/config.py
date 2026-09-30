"""Runtime configuration shared by every task.

The same code runs locally (plain Spark + Delta, file streaming) and on Databricks
(Unity Catalog, Volumes, Auto Loader). Only this object differs between the two.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Config:
    # Root folder holding source/, staging/, landing/ and _checkpoints/.
    base_path: str
    # Unity Catalog catalog; None means the local spark_catalog (two-level names).
    catalog: str | None = None
    # Use Auto Loader (cloudFiles) for file ingestion. Only available on Databricks.
    use_autoloader: bool = False
    run_id: str = "local"

    # --- replay plan -------------------------------------------------------
    # Everything that happened before this date is delivered as one backfill batch
    # dated backfill_date; afterwards one batch per day for replay_days days.
    backfill_date: dt.date = dt.date(2018, 5, 31)
    replay_days: int = 10
    seed: int = 42
    # Optional date whose batch is deliberately poisoned to demonstrate the DQ gate.
    poison_date: dt.date | None = None

    # --- data quality policy (see ADR-0004 / ADR-0005) ----------------------
    late_tolerance_days: int = 3
    dq_max_bad_ratio: float = 0.05

    schemas: dict = field(default_factory=lambda: {
        "bronze": "olist_bronze",
        "silver": "olist_silver",
        "gold": "olist_gold",
        "ops": "olist_ops",
    })

    # ---------------------------------------------------------------------
    def table(self, layer: str, name: str) -> str:
        schema = self.schemas[layer]
        return f"{self.catalog}.{schema}.{name}" if self.catalog else f"{schema}.{name}"

    def schema_name(self, layer: str) -> str:
        schema = self.schemas[layer]
        return f"{self.catalog}.{schema}" if self.catalog else schema

    @property
    def source_path(self) -> str:
        return f"{self.base_path}/source"

    @property
    def staging_path(self) -> str:
        return f"{self.base_path}/staging"

    @property
    def landing_path(self) -> str:
        return f"{self.base_path}/landing"

    def checkpoint(self, name: str) -> str:
        return f"{self.base_path}/_checkpoints/{name}"

    @property
    def first_replay_date(self) -> dt.date:
        return self.backfill_date + dt.timedelta(days=1)

    @property
    def last_replay_date(self) -> dt.date:
        return self.backfill_date + dt.timedelta(days=self.replay_days)
