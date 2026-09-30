# ADR-0003: Hand-written SCD Type 2 for sellers from daily snapshots

[日本語](../ja/0003-scd2-sellers.md) | **English** | [中文](../zh/0003-scd2-sellers.md)

**Status:** accepted

## Context
The sellers feed is a full snapshot every day. Sellers occasionally move city or state.
Delivery performance by region has to use where the seller was *when the order was placed*,
not where they are today.

## Decision
- `silver.seller_history` has `seller_sk`, `valid_from`, `valid_to` (exclusive) and `is_current`.
- Change detection compares a SHA-256 hash of the tracked attributes (zip prefix, city, state).
- The whole snapshot is applied with a single MERGE, using the classic trick of staging each
  changed row twice. The row with `merge_key = seller_id` closes the old version; the row with
  `merge_key = NULL` never matches and is inserted as the new version.
- The first known version is valid from `1900-01-01`. We have no history before the first
  snapshot, and assuming the first-known attributes always held is better than orders
  matching no seller.
- A seller missing from a snapshot stays current. A snapshot is not a delete signal.
- Several snapshots in one micro-batch (after an outage) are applied in date order.
- The fact table joins on `valid_from <= order_date < valid_to`.

## Alternatives considered
- **SCD1 (overwrite):** loses the history that the regional metrics need.
- **Lakeflow `AUTO CDC ... STORED AS SCD TYPE 2`:** correct and shorter, but hides exactly
  the mechanics this project is meant to show (ADR-0008).

## Consequences
- Re-applying the same snapshot is a no-op because the hashes are equal.
- Tests: `test_scd2_keeps_history_and_is_idempotent`,
  `test_fact_uses_the_seller_version_valid_on_the_order_date`, and the e2e check that closed
  versions equal the injected relocations.
