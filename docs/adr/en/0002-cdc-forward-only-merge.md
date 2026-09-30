# ADR-0002: Apply order CDC forward-only by `change_seq`

**Status:** accepted

## Context
Each status change of an order arrives as its own row (an after-image). Changes can arrive
out of order: the `approved` change can land two days after `shipped`. The same change can
also arrive twice. File order and arrival time say nothing about which change is newest.

## Decision
- Within a micro-batch, keep the row with the highest `change_seq` per `order_id`.
- MERGE into `silver.orders` with `WHEN MATCHED AND s.change_seq > t.change_seq THEN UPDATE`.
  A change that is older than or equal to what we already hold is ignored.
- Change rows that fail validation (for example, delivered before purchase) are quarantined.
  The order keeps its last valid state.

## Alternatives considered
- **Order by `change_ts`:** timestamps can tie, and clocks on different systems drift. The
  producer's sequence number is the only reliable ordering.
- **Keep the full change log in silver and derive the latest state in a view:** useful for
  auditing, but the replay has it in bronze already. Silver's job is current state.
- **Declarative `AUTO CDC ... SEQUENCE BY change_seq` (Lakeflow):** same semantics in one
  line. See ADR-0008 for why this project writes it by hand.

## Consequences
- Status never regresses. Test: `test_late_older_change_does_not_regress_status`, plus the
  e2e check that silver matches the ground truth computed from the replay staging data.
- If the producer ever resets `change_seq`, this breaks. That would be a contract change.
