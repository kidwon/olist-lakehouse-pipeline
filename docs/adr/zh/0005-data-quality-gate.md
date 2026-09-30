# ADR-0005：隔离、指标，以及 Gold 之前的质量闸门

[日本語](../ja/0005-data-quality-gate.md) | [English](../en/0005-data-quality-gate.md) | **中文**

**状态：** 已采纳

## 背景
坏数据一定会来：负价格、缺失或不存在的商品、不可能的时间戳。需要回答三个问题：坏数据去哪里？谁会发现？管道应该在什么时候停下？

## 决策
- **规则：** 以命名条件的形式定义（`quality.py`）。每一行都会带上 `_failed_rules`，列出它违反的所有规则。
- **隔离：** 坏行连同违反的规则、解析后的记录、原始行和来源文件一起写入 `ops.quarantine`，不会被悄悄丢弃。
- **指标：** 每个微批次按规则、按批次日期把命中数写入 `ops.dq_metrics`。`__all__` 表示至少违反一条规则的行数。严重级别分为 `error`（规则违反）、`warn`（`rescued_data`）和 `info`（重复）。
- **闸门：** Silver 之后，`dq_gate` 按表计算本次 `run_id` 的"坏行数 ÷ 总行数"。超过 **5%** 时任务失败，Gold 和 mart 不发布。每次判定都记录在 `ops.dq_gate_log`。
- 闸门**不会**回滚 Silver。好行本身是正确的，坏行已经被隔离。下一次正常运行时，会基于 Silver 发布 Gold。

## 考虑过的备选方案
- **丢弃坏行，只记日志：** 没人看日志，数据也丢了。
- **只要有一行坏数据就失败：** 真实数据总会有一些坏行，管道将永远跑不完。
- **Lakeflow 的 expectations：** `expect_or_drop` 和 `expect_or_fail` 能表达规则和强制停止，但"按整次运行的比例阈值"和"可查询的隔离表"最终还是要自己实现。

## 影响
- 端到端测试把某一天污染成约 12% 的坏价格，只有那一次运行被拦下，之后的运行中 Gold 会追上。
- 测试：`test_metrics_count_bad_rows_once_and_each_rule_separately`、`test_gate_blocked_only_the_poisoned_run`，以及核对每一类注入异常的端到端测试。
