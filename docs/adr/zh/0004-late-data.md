# ADR-0004：迟到数据按销售发生日归日（容忍 3 天）

[日本語](../ja/0004-late-data.md) | [English](../en/0004-late-data.md) | **中文**

**状态：** 已采纳

## 背景
部分订单明细在销售发生后 1 到 2 天才到达，极少数晚到一周。报表应该把销售算在它实际发生的那天；但如果无限期地回溯修改历史，已经报告过的数字就会一直不稳定。

## 决策
- 每条明细都带有 `event_date`（下单日期）和 `arrival_lag_days`（批次日期减下单日期），Gold 以 `event_date` 为准。
- **3 天以内**的迟到：接受。Gold 事实表按粒度键 MERGE，迟到的明细会落到原本的日期，受影响那几天的数字随之修正。
- **超过 3 天**的迟到：按规则 `too_late` 隔离，并计入 `dq_metrics`。这样由人有意识地做决定，而不是让历史数据悄悄变化。
- 首次回填批次不受此规则约束，因为它本来就是用来传送历史数据的。

## 考虑过的备选方案
- **按到达日期归日：** 数字稳定，但每日销售额是错的，而且错得没人发现。
- **无论多晚都接受：** 理论上最准确，但月底的数字可能在几周后还在变。

## 影响
- 3 天这个容忍期是业务决策，配置在 `Config.late_tolerance_days`。
- 测试：`test_each_bad_row_carries_every_rule_it_breaks`（3 天与 4 天的边界）、`test_backfill_history_is_not_flagged_late`、`test_late_items_are_accepted_and_dated_by_the_sale`（端到端）。
