# ADR-0006: Explicit data contracts; unknown fields are rescued, not inferred

**Status:** accepted

## Context
Schema inference on JSON lets the producer change our tables: a new field becomes a new
column, a type flip breaks downstream code, and nobody decided either. Part-way through the
replay, the items producer starts sending `discount_amount`.

## Decision
- Every feed has a declared schema in `contracts.py`.
- JSON lines are read as text and parsed with `from_json(contract)`. Any key that is not in the
  contract is kept in `_rescued_data` as JSON. A line that is not a JSON object sets
  `_malformed`.
- Bronze keeps the raw line (`_raw`), so a future contract version can backfill from bronze
  without re-ingesting files.
- Rows with rescued fields are counted as `rescued_data` (severity `warn`). Adopting a new
  field is a deliberate contract change.

## Alternatives considered
- **Auto Loader `addNewColumns`:** evolves the schema automatically, and the stream restarts.
  Convenient, but the decision then belongs to the producer.
- **Auto Loader `rescue` mode:** same idea as ours, but only available on Databricks. Our
  parser behaves the same locally and in CI.

## Consequences
- Test: `test_unknown_fields_are_rescued_not_dropped`. In the e2e run, rescued rows equal the
  rows that carried the new field.
