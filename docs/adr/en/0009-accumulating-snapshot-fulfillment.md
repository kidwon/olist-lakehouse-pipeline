# ADR-0009: Accumulating snapshot fact for order fulfillment

[日本語](../ja/0009-accumulating-snapshot-fulfillment.md) | **English** | [中文](../zh/0009-accumulating-snapshot-fulfillment.md)

**Status:** accepted

## Context
`fact_order_item` is a transaction fact: one row per order line, which answers "what sold". It
can't answer "where does the time go between purchase and delivery", because that is a property
of the order's whole lifecycle: purchase → approval → handed to the carrier → delivered, or
cancelled / unavailable.

## Decision
- **`gold.fact_order_fulfillment`**: an accumulating snapshot, **one row per order**, updated in
  place as milestones arrive (MERGE on `order_id`, rewriting a row only when its content hash
  changed).
- **Source: a new `silver.order_changes`.** It is an append-only log of every validated status
  change, keyed `(order_id, change_seq)` and never updated. `silver.orders` keeps only the
  current state; the log keeps the path, which gives the end time of cancelled/unavailable
  orders and doubles as an audit trail. It runs as its **own stream with its own checkpoint**,
  so an existing deployment backfills the whole history from bronze on its next run. Rules are
  re-evaluated only to filter; quarantine and metrics are still recorded once, by `orders`.
- **Milestones** come from the order's latest change (an after-image carries every timestamp
  known so far). The end is the first `canceled` / `unavailable` change, with `end_reason`.
- **Durations in hours**: `hours_to_approve`, `hours_to_ship`, `hours_in_transit`,
  `hours_total`, and `hours_late`, which counts from the **end** of the estimated day, because the
  estimate is a date (consistent with `is_on_time`).
- **Role-playing date keys**: one key per milestone, all pointing at `dim_date`. Following
  Kimball, a date key is **never NULL**: a milestone that has not happened yet points at a
  special `dim_date` row, `-1` ("not yet happened"). Joins never drop open orders, and BI tools
  show a label instead of a blank.
- **No seller column.** One order can have several sellers; seller analysis stays on
  `fact_order_item`. Order-level measures are `item_count` and `order_value`.

## Inconsistent timestamps
A stage duration that comes out negative is set to NULL and the order gets
`has_inconsistent_milestones`; the raw timestamps are kept as delivered. A warn-level metric,
`inconsistent_milestones`, is written to `ops.dq_metrics` on every run (the gate ignores it).

On the real data, 628 of 82,406 orders (0.76%) are flagged:

| Inconsistency | Orders |
|---|---:|
| Handed to the carrier before approval | 559 |
| Handed to the carrier before the purchase | 46 |
| Delivered before being handed to the carrier | 23 |

Most are minutes apart, which looks like clock skew between systems; a few are months apart.

**Known gap:** the silver rule `change_before_purchase` quarantines only the change it fires on.
Later changes are after-images that still carry the wrong carrier timestamp, so it reaches
`silver.orders` (46 orders). This ADR handles it in gold with the flag instead of changing silver
behaviour; fixing silver would mean nulling individual timestamps there, which is a separate
decision.

## Satisfaction next to the delay
Added after a reader asked how delays affect customer satisfaction. The fact carries
`review_score` (the mean of the order's reviews; an order can have more than one) and
`review_count`. An order without a review keeps a NULL score; nothing is imputed. On the real
data, delivered and reviewed orders:

| Delivery | Orders | Avg score | 1–2 star share |
|---|---:|---:|---:|
| On time | 71,451 | 4.27 | 9.4% |
| Late | 5,505 | 2.21 | 64.1% |

By lateness: 7+ days early 4.30, 0–7 days early 4.08, 1–3 days late 3.22, 4–7 days late 2.07,
8+ days late 1.66. Olist has no other customer touchpoints (support contacts, browsing), so this
covers the delivery experience only. Reviews arrive after delivery, so the most recent orders
have no score yet (the same right-censoring as the stage durations).

## Alternatives considered
- **Read milestones from `silver.orders` only:** simpler, but there is no end time for
  cancelled orders and no history.
- **Read bronze directly:** unvalidated data in gold.
- **Treat small negative gaps as clock skew (clamp to 0 within N hours):** N would be invented.
  Kept as an option if the business defines a tolerance.
- **Exclude flagged orders:** silently drops 0.76% of orders.

## Consequences
- Right-censoring: for recent months only orders that finished quickly have completed stages,
  so their averages are biased low. The dashboard says so.
- Making gold incremental with Change Data Feed is the next step, for both fact tables at once.
- Tests: `tests/test_fulfillment.py` (milestones, hours, date keys, on-time boundary, open orders,
  end reasons, inconsistent timestamps, in-place update, append-only log) and two e2e checks.
