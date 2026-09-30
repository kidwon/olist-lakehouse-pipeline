# ADR-0001: Idempotent ingestion and de-duplication

[日本語](../ja/0001-idempotent-ingestion-and-dedup.md) | **English** | [中文](../zh/0001-idempotent-ingestion-and-dedup.md)

**Status:** accepted

## Context
The order-items feed is append-only, but the producer re-sends lines: sometimes twice in the
same file drop, sometimes again the next day. Jobs also get retried. If any of these reaches
the fact table, revenue is double counted.

## Decision
Duplicates are removed at two levels, and each level catches a different case.

1. **File level:** bronze reads with Auto Loader (locally, the file-stream source) using
   `trigger(availableNow=True)` and a checkpoint. A file is ingested exactly once, so a job
   retry finds nothing new.
2. **Row level, inside a batch:** `keep_first` uses `row_number()` over the business key
   `(order_id, order_item_id)`.
3. **Row level, across batches:** the silver MERGE only has `WHEN NOT MATCHED THEN INSERT`.
   A key we already hold is treated as a re-delivery, never as an update.

Both row-level counts are written to `ops.dq_metrics` as `duplicate_in_batch` and
`duplicate_already_loaded` (severity `info`), so the duplicates are visible and not just
silently absorbed.

## Alternatives considered
- **`dropDuplicates` with a watermark in the stream:** needs a time bound on how late a
  duplicate can arrive, and keeps state. MERGE against the table has no time bound.
- **Overwrite silver from bronze on every run:** simple, but the cost grows with history, and
  retries would still reprocess everything.

## Consequences
- Correcting a line would need a new key or an explicit update feed. That matches the source
  contract ("rows are only ever added").
- Test: `test_redelivered_item_is_not_counted_twice`, plus the e2e check that silver equals
  delivered minus duplicates minus quarantine.
