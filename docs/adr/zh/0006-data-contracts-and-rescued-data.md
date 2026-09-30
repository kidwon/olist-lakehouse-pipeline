# ADR-0006：明确的数据契约；未知字段转存而不是推断

[日本語](../ja/0006-data-contracts-and-rescued-data.md) | [English](../en/0006-data-contracts-and-rescued-data.md) | **中文**

**状态：** 已采纳

## 背景
如果依赖 JSON 的 schema 推断，上游就能改变我们的表：新字段变成新列，类型一变下游代码就坏，而这些都不是谁有意决定的。回放进行到一半时，明细数据的上游开始发送 `discount_amount`。

## 决策
- 每个数据源在 `contracts.py` 里都有明确的 schema。
- JSON Lines 先按文本读取，再用 `from_json(契约 schema)` 解析。契约之外的键以 JSON 形式转存到 `_rescued_data`。不是 JSON 对象的行会标记 `_malformed`。
- Bronze 保留原始行（`_raw`），将来修订契约时可以直接从 Bronze 回填，不需要重新摄取文件。
- 带有转存字段的行计为 `rescued_data`（严重级别 `warn`）。是否采用新字段，是一次有意的契约变更。

## 考虑过的备选方案
- **Auto Loader 的 `addNewColumns`：** 自动扩展 schema 并重启流。方便，但决定权交给了上游。
- **Auto Loader 的 `rescue` 模式：** 思路相同，但只能在 Databricks 上运行。本项目的解析器在本地和 CI 上行为一致。

## 影响
- 测试：`test_unknown_fields_are_rescued_not_dropped`。端到端测试中，被转存的行数与带有新字段的行数一致。
