# ADR-0010: Periodic snapshot of the daily order backlog

[日本語](../ja/0010-periodic-snapshot-backlog.md) | **English** | [中文](../zh/0010-periodic-snapshot-backlog.md)

**Status:** accepted

## Context
The project had two of Kimball's three fact types: a transaction fact (`fact_order_item`) and an
accumulating snapshot (`fact_order_fulfillment`, ADR-0009). Neither answers "how many orders were
stuck at each stage on a given day, and is that getting worse?" That is a question about **state
at regular intervals**, which is what a periodic snapshot is for. Olist has no inventory or
balance feed, but the append-only change log (`silver.order_changes`) can be replayed into the
status of every order on any past day.

## Decision
- **`gold.fact_daily_order_backlog`**: one row per **day × open status × customer state**, from the
  first order (2016-09-04) to the last day in the log. Open statuses are created, approved,
  invoiced, processing and shipped; delivered, cancelled and unavailable orders leave the backlog.
- An order's status on day D is the status of its highest `change_seq` among the changes dated
  on or before D. It is counted from its purchase date, so a change dated before the purchase
  never makes it appear early.
- **Measures**: open orders, their value, median age in days, orders open more than 30 days and
  orders past their estimated delivery date. The measures are semi-additive: they add across
  states and statuses, never across days.
- **Customer state** rather than seller: an order has one customer but possibly several sellers.
  A missing state becomes `unknown`, never NULL, because a NULL merge key never matches and would
  duplicate every run.
- **Stuck orders stay**: 677 real orders stop in processing / invoiced / created and never close.
  They are kept and made measurable (open over 30 days, overdue), and medians keep them from
  distorting age.

## By event time, not arrival time
The day is the day a change **happened**. Kimball's classic periodic snapshot writes each period
once and never changes it ("as known at the end of the period"). That is deliberately not done
here:
- 98% of the history (316,276 of 323,868 changes) arrived in the single backfill batch, so
  "as known on the arrival day" would pile two years onto one day.
- A late change belongs to the day it happened, so the days already published must be corrected.

The snapshot is rebuilt from the log on every run and MERGEd on its grain: changed rows are
updated, new combinations inserted, and **combinations that no longer exist are deleted**
(`whenNotMatchedBySourceDelete`). Every restatement is visible in the Delta history.

## Results on the real data
- 53,836 rows; on the last day 3,827 open orders, exactly the open orders of
  `fact_order_fulfillment`.
- The backlog peaked at 5,428 open orders on 2018-03-25.
- Overdue orders climb from about 30 a day in early 2017 to about 1,700 a day in April 2018.
  Many orders are marked shipped but never get a delivery date, so they accumulate. On the last
  day 1,150 shipped orders are overdue.

## Alternatives considered
- **Arrival time, append-only:** the classic definition, but meaningless history with this data.
- **One row per open order per day:** tens of millions of rows for the same answers.
- **Overwrite the table every run:** simpler, but restatements become invisible.
- **Drop orders open longer than N days:** N would be invented, and it hides real problems.

## Consequences
- The whole history is recomputed every run (a few seconds at this size). Limiting the rebuild
  to the days affected by new changes is part of the Change Data Feed step that comes next.
- Tests: `tests/test_backlog.py` (status at the end of the day, leaving the backlog, measures,
  stuck orders, changes before the purchase, NULL-safe keys, restatement with deletes) and two
  e2e checks: an idempotent rerun, and the last day equal to the open orders of the fulfillment
  fact.
