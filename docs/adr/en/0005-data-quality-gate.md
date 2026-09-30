# ADR-0005: Quarantine, metrics and a gate before gold

**Status:** accepted

## Context
Bad rows will arrive: negative prices, missing or unknown products, impossible timestamps.
Three questions need answers: where does a bad row go, who finds out, and when should the
pipeline stop?

## Decision
- **Rules** are named conditions (`quality.py`). Each row gets `_failed_rules`, the list of
  every rule it breaks.
- **Quarantine:** bad rows go to `ops.quarantine` with their rules, the parsed record, the raw
  line and the source file. Nothing is dropped silently.
- **Metrics:** every micro-batch writes hit counts per rule and batch date to `ops.dq_metrics`.
  `__all__` counts rows that broke at least one rule. Severities: `error` (rules), `warn`
  (`rescued_data`), `info` (duplicates).
- **Gate:** after silver, `dq_gate` computes bad rows divided by total rows per table for this
  `run_id`. Above **5%**, the task fails, so gold and the mart are not published. Every
  evaluation is logged in `ops.dq_gate_log`.
- The gate does **not** roll back silver. Good rows are correct on their own, and bad rows are
  already isolated. The next clean run publishes gold from silver.

## Alternatives considered
- **Drop bad rows and only log them:** nobody reads logs, and the data is gone.
- **Fail on any bad row:** real data always has some bad rows, and the pipeline would never
  finish.
- **Lakeflow expectations:** `expect_or_drop` and `expect_or_fail` cover rules and hard stops,
  but a ratio threshold across a whole run plus a queryable quarantine are custom anyway.

## Consequences
- The e2e test poisons one day with about 12% bad prices. Only that run is blocked, and a
  later run catches gold up.
- Tests: `test_metrics_count_bad_rows_once_and_each_rule_separately`,
  `test_gate_blocked_only_the_poisoned_run`, and the e2e reconciliation of every injected
  anomaly.
