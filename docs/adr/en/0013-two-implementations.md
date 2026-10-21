# ADR-0013: Two complete implementations, reconciled every day

[日本語](../ja/0013-two-implementations.md) | **English** | [中文](../zh/0013-two-implementations.md)

**Status:** accepted. Supersedes the decision of ADR-0012 (declarative only as a reference).

## Context
ADR-0012 rebuilt four mechanisms and one fact in Lakeflow Spark Declarative Pipelines and kept
the imperative pipeline as the only real implementation. Its reasons were three gaps: a business
rule (the first seller version starts in 1900), a gate that blocks only gold, and no local tests.
Writing the declarative version showed how much less there is to maintain, so the goal changed:
build the whole spec declaratively, as a real implementation.

## Decision
- **Both implementations are complete and equal.** `lakeflow/` now builds every table the
  imperative code builds: bronze, the rule checks, quarantine, all silver tables, the metrics, the
  gate, every dimension, the three fact types and the mart. It writes to `olist_sdp`.
- **They run side by side every day.** `olist_daily` delivers the batch, runs the imperative tasks
  and the declarative pipeline in parallel, then `olist reconcile` compares 16 tables row by row
  (`EXCEPT ALL` in both directions) and fails the job on any difference.
- **The imperative version is the reference.** The declarative pipeline cannot run on a laptop;
  the imperative one is unit- and end-to-end-tested locally. The daily reconciliation is how the
  declarative version is tested.
- **One dashboard per implementation**, generated from the same script.

## How the gaps were closed
| Gap (ADR-0012) | Declarative solution |
|---|---|
| Rules written twice (expectations + quarantine) | Each `checked_*` table lists the rules a row breaks in `_failed_rules`, written once. Expectations only reference them by name (no action), so the event log still counts every rule; quarantine and silver split the rows. |
| First seller version starts at the first snapshot (97% of fact rows had no seller) | `dim_seller` reshapes the `AUTO CDC FROM SNAPSHOT` history: the earliest version starts at 1900-01-01 and `seller_sk = xxhash64(seller_id, valid_from)`, the same key as the imperative version. |
| The gate stops the whole update, not only gold | The job refreshes the pipeline twice with `refresh_selection`: bronze, silver, quarantine, metrics and `dq_gate` first, gold second. `dq_gate` fails its refresh above 5%, so the gold refresh never starts. Verified on Free Edition: tables outside the selection are `EXCLUDED`, and silver written in a failed refresh is kept and not written twice on the next one. A pipeline retries a failed update by default, which only delays a data failure, so `pipelines.numUpdateRetryAttempts` is 0. With the threshold lowered to 0.1% as a test, the job failed in under 3 minutes and `sdp_gold` was skipped (`UPSTREAM_FAILED`). |
| The gate re-checked every batch ever received | `dq_gate` reads only the latest batch. |
| No local tests | The daily reconciliation against the tested imperative version. |
| Incremental gold written by hand (ADR-0011) | Gold tables are materialized views; the platform chooses how to refresh each one and records it in the event log (published as `olist_sdp.event_log`). A full refresh recomputes; an update with nothing new upstream is `NO_OP`. |

## What reconciling found
1. **Re-deliveries ordered by ingestion time.** The first full refresh differed in 10 order
   items, and in the fact built from them. A full refresh ingests every day at once, so all rows
   share one ingestion time and "keep the first delivery" picked a copy at random; the copy from
   the next day carries an arrival lag one day too large. Both implementations now order
   deliveries by the delivery date in the file path (`first_delivery` / `latest_delivery` in
   `silver.py`, the `SEQUENCE BY` of the declarative flows). The imperative order items were only
   correct by luck: their tie-breaker, the file path, starts with the date. Customers and reviews
   had no date ordering at all.
2. After that fix, **all 16 checks are identical**: 6 silver tables, 4 dimensions, 3 facts, the
   mart, quarantine and the rule metrics.

## Differences that remain (by design)
- **Gate semantics.** The imperative gate judges the input of this run; a run with no new data
  passes. The declarative gate judges the latest batch, so a bad batch keeps gold frozen until the
  data is fixed. Both protect gold; the declarative one is stricter.
- **No gate log.** The declarative dashboard shows the gate per batch from the metrics, not per run.
- **Quarantine and metrics are compared as sets**, and the de-duplication counts (info) are not
  compared: the imperative ops tables append again when a batch is reprocessed.
- **`refresh_selection` is a beta feature** of the job's pipeline task.

## Size
About 550 lines of SQL and Python declaratively, against about 1,070 lines of Python for the
same scope imperatively (bronze, quality, silver, SCD2, gate, gold, incremental), comments and
blank lines excluded. The declarative SQL is longer than ADR-0012's 145 lines because it now holds
the whole spec, including JSON parsing identical to the imperative bronze.

## Consequences
- `olist_daily` has three more tasks: `sdp_silver_and_gate`, `sdp_gold`, `reconcile`. Results are
  appended to `olist_ops.reconcile_log`.
- `tests/test_pipeline_e2e.py` checks that every reconciled column exists in the imperative
  tables, so a rename fails locally instead of in the daily job.
- Walkthrough notebook 10 runs the reconciliation and shows how the gaps were closed.
