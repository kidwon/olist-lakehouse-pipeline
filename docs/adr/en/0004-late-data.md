# ADR-0004: Late data is dated by the sale, within a 3-day tolerance

**Status:** accepted

## Context
Some order items arrive one or two days after the day they were sold, and a few arrive a
week late. A report should put a sale on the day it happened. On the other hand, reopening
old periods forever makes numbers that were already reported unstable.

## Decision
- Every item carries `event_date` (the purchase date) and `arrival_lag_days`
  (batch date minus event date). Gold uses `event_date`.
- Up to **3 days** late: accepted. The gold fact is MERGEd on its grain, so a late item lands
  in its original date and the affected days are corrected.
- More than 3 days late: quarantined with rule `too_late` and counted in `dq_metrics`, so
  someone decides consciously instead of the history changing silently.
- The backfill batch is exempt. It carries history by design.

## Alternatives considered
- **Date by arrival:** stable, but daily sales would be wrong, and wrong in a way nobody sees.
- **Accept any lateness:** correct in theory, but month-end numbers could change weeks later.

## Consequences
- The 3-day tolerance is a business decision and lives in `Config.late_tolerance_days`.
- Tests: `test_each_bad_row_carries_every_rule_it_breaks` (the 3-vs-4-day boundary),
  `test_backfill_history_is_not_flagged_late`, and
  `test_late_items_are_accepted_and_dated_by_the_sale` (e2e).
