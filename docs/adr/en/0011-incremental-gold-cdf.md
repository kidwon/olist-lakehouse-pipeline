# ADR-0011: Incremental gold with Change Data Feed

[日本語](../ja/0011-incremental-gold-cdf.md) | **English** | [中文](../zh/0011-incremental-gold-cdf.md)

**Status:** accepted

## Context
Until v0.3.0, every run rebuilt the gold facts from all of silver and MERGEd the result, rewriting
only rows whose hash changed. A daily batch changes one or two thousand silver rows out of
~100k, so almost all of the reading and joining was repeated work. It costs little at this size
(gold runs in about 90 seconds on serverless, much of it start-up), but it grows with the history.

## Decision
- **Change Data Feed on the silver sources.** Gold switches it on (idempotently) for every table
  it reads. Changes are only recorded from that moment, so a source without a watermark always
  starts with a full rebuild.
- **Watermarks per (fact, source)** in `ops.gold_watermarks`. A run first fixes the current
  version of each source as its upper bound, reads the changes in `(watermark, upper bound]`, and
  saves the upper bound as the new watermark **only after the fact's MERGE succeeded**. A failed
  run is processed again next time; nothing is skipped.
- **Changes become affected orders**, including indirect ones, and only those orders are rebuilt
  from silver and MERGEd:

  | Fact | Sources and how a change maps to orders |
  |---|---|
  | `fact_order_item` | items and orders → their order; a customer → that customer's orders; a new seller version → that seller's orders |
  | `fact_order_fulfillment` | order changes, items and reviews → their order; a customer → that customer's orders |

- **Full rebuild instead** when the target does not exist, a source has no watermark, the change
  data for the needed versions is gone (Delta keeps it only for a retention period), or the job
  runs with `full_refresh=true`. The reason is recorded.
- **Every run is measured**: mode, reason, orders rebuilt and total orders go to
  `ops.gold_incremental_stats`, shown on the dashboard.
- **The daily backlog snapshot stays a full rebuild.** It aggregates by day, and a late change can
  restate days long past; working out which days to recompute is a different and harder problem
  than "which orders", while the full rebuild takes seconds. Revisit when it becomes slow.

## Results on the real data
- After 11 runs, both facts are **identical to a full rebuild** (93,465 and 82,406 rows, 0
  differing).
- The first run is full; every later run rebuilds **0.44%–1.89%** of the orders, about 1% on a
  typical day.
- At ~100k rows the wall-clock gain is small, because serverless start-up dominates. The value is
  the pattern and the work that no longer grows with the history.

## Alternatives considered
- **Streaming the change feeds with checkpoints:** natural for one source, awkward for a fact
  that depends on four; a watermark table keeps one place to look and one commit point.
- **Incremental MERGE keyed directly on changed rows** (no order mapping): misses indirect effects,
  e.g. a customer change that must update that customer's orders.
- **Make the backlog incremental too:** see above; deferred on purpose.

## Consequences
- Correctness is guarded three ways: tests compare the incremental result with a full rebuild
  after every replayed day (including the gate-blocked day and the catch-up run), a
  `full_refresh` switch exists, and the incremental path falls back to full whenever it cannot be
  trusted.
- Tests: `tests/test_incremental.py` (first run full then incremental, nothing new, full refresh
  wins, missing change data falls back, watermark moves only after success, stats) and two e2e
  checks (equality with a full rebuild, incremental runs after the first).
