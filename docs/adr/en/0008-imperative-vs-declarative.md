# ADR-0008: Imperative Structured Streaming + MERGE rather than Lakeflow Declarative Pipelines

[日本語](../ja/0008-imperative-vs-declarative.md) | **English** | [中文](../zh/0008-imperative-vs-declarative.md)

**Status:** accepted

## Context
Databricks offers two ways to build this: Jobs with Structured Streaming and hand-written
MERGE (imperative), or Lakeflow Spark Declarative Pipelines with `AUTO CDC` and expectations
(declarative). Both were covered in the course this project builds on.

## Decision
Imperative. The purpose of this repository is to show the mechanics: idempotent MERGE,
forward-only CDC, SCD2 and quarantine. Declarative pipelines turn each of these into one
keyword. The imperative code is also plain PySpark, so every transformation runs under
pytest on a laptop and in CI.

## What it would look like in Lakeflow
| Here | Lakeflow equivalent |
|---|---|
| Auto Loader + checkpoint in `bronze.py` | `@dp.table` over `STREAM read_files(...)` |
| `merge_cdc_forward_only` | `AUTO CDC INTO orders ... KEYS (order_id) SEQUENCE BY change_seq` |
| `scd2.apply_snapshot` | `AUTO CDC FROM SNAPSHOT ... STORED AS SCD TYPE 2` |
| rules + quarantine | `@dp.expect_all_or_drop` plus a separate quarantine table with the inverted rules |
| `dq_gate` | no direct equivalent; a separate job task reading the event log |

## Consequences
- More code to own, but each part is small and tested.
- In a team setting, Lakeflow would be the default for new pipelines. The concepts carry over
  one-to-one.
