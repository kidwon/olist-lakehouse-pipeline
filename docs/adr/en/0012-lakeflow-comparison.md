# ADR-0012: The same spec in Lakeflow Declarative Pipelines, side by side

[日本語](../ja/0012-lakeflow-comparison.md) | **English** | [中文](../zh/0012-lakeflow-comparison.md)

**Status:** accepted

## Context
ADR-0008 chose imperative Structured Streaming + MERGE to show the mechanics, and claimed each of
them would be "one keyword" in Lakeflow. This ADR tests that claim. The four mechanisms were
rebuilt declaratively in `lakeflow/` (SQL, plus Python where SQL can't do it): ingestion,
forward-only CDC, seller SCD2 from snapshots, and order-item quality with quarantine, plus the
gold transaction fact. The pipeline reads **the same landing files** and writes to its own
schema, `olist_sdp`, so the two can be reconciled. It is triggered, not continuous: Free Edition
allows one active pipeline per type.

## Reconciliation on the real data
| Check | Imperative | Lakeflow | Result |
|---|---:|---:|---|
| Silver orders (rows; id + status + change_seq) | 82,406 | 82,406 | identical |
| Silver order items (rows; keys) | 93,465 | 93,465 | identical |
| Quarantined items per rule (missing / price / too late / unknown) | 9 / 11 / 5 / 3 | 9 / 11 / 5 / 3 | identical |
| Gold fact rows (key fields) | 93,465 | 93,465 | identical |
| Gate: highest bad-row ratio | 2.11% | 2.11% | identical |
| Seller history (rows; closed versions) | 3,173; 78 | 3,173; 78 | identical (after the fix below) |
| Fact rows without a seller version | 0 | 91,085 (97%) | **differs by design** |

## Where Lakeflow is simpler
- **Code size:** about 145 lines (SQL + Python) against about 450 for the same mechanisms
  imperatively. Not a strict one-to-one comparison, but the gap is real.
- **Forward-only CDC** is one `AUTO CDC ... SEQUENCE BY change_seq` flow; checkpoints and the
  MERGE condition disappear.
- **Deduplication** is `AUTO CDC` keyed on the business key.
- **Quality metrics come for free:** the event log records passed and failed rows per
  expectation, and the counts match `ops.dq_metrics` exactly.
- **Snapshots are versioned:** `AUTO CDC FROM SNAPSHOT` only applies snapshots newer than the last
  one. The reconciliation showed this protects against something the imperative version missed
  (next section).

## What the comparison found in the imperative version
The setup job was run again by hand. That reset the replay and re-delivered the 2018-05-31
backfill files. The imperative SCD2 treated the old snapshot as a change and moved 78 relocated
sellers back to their old cities, creating versions that ended before they began. Lakeflow
ignored the re-delivered snapshot. Fixes in the imperative version:
- `scd2.apply_snapshot` skips any snapshot not newer than the last applied one (a table
  property), with a regression test that fails without the fix.
- `replay.prepare` refuses to reset a replay that has delivered batches unless `--force` is given.
- On Databricks the table was rolled back with `RESTORE TABLE ... TO VERSION AS OF 12` (Delta time
  travel) and gold was rebuilt with `full_refresh=true`.

## Where Lakeflow needs workarounds or can't follow the spec
- **First SCD2 version.** The spec (ADR-0003) starts a seller's first version at 1900-01-01.
  Lakeflow starts it at the first snapshot, so **97% of fact rows (orders before 2018-05-31) find
  no seller version.** A business rule like this needs an extra layer on top, which gives back
  much of the simplicity.
- **Historical snapshots need Python.** The SQL form snapshots a table as it is now; replaying one
  snapshot per day in order needs `create_auto_cdc_from_snapshot_flow` with a version function.
- **Quarantine needs a second table.** Expectations drop rows and count them, but don't keep them.
  The same rules, inverted, write a quarantine table, so every rule is written twice.
- **The gate is an emulation.** An `EXPECT ... ON VIOLATION FAIL UPDATE` on an aggregate stops the
  whole update, not only gold. It also re-evaluates every batch ever received on each update, so
  one bad historical batch would fail every later update until it is handled.
- **No local tests.** The pipeline runs only on Databricks; the imperative code runs under pytest
  on a laptop and in CI (64 tests plus notebooks). Here, correctness rests on reconciliation.

## Decision
Keep the imperative pipeline as the primary implementation in this repository: it is testable
end to end, and it encodes business rules (the 1900 start, the ratio gate that blocks only gold)
exactly. The Lakeflow version stays as a reference implementation and a reconciliation check.
For a new team pipeline without such rules, Lakeflow would be the default: far less code, and
metrics and snapshot versioning for free.

## Consequences
- `resources/olist_sdp.yml` deploys the pipeline with the bundle; walkthrough notebook
  [`10_lakeflow_comparison`](../../../notebooks/walkthrough/10_lakeflow_comparison.py) runs the
  reconciliation on Databricks.
- ADR-0003 is amended: snapshots must move forward, and now the code enforces it.
