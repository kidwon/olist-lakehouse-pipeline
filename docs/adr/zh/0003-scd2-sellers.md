# ADR-0003：基于每日快照手写卖家的 SCD Type 2

[日本語](../ja/0003-scd2-sellers.md) | [English](../en/0003-scd2-sellers.md) | **中文**

**状态：** 已采纳

## 背景
卖家数据源每天提供一份全量快照，卖家偶尔会迁址（换城市或州）。按地区统计配送表现时，要用卖家**下单当时**所在的地点，而不是现在的地点。

## 决策
- `silver.seller_history` 包含 `seller_sk`、`valid_from`、`valid_to`（不含当天）和 `is_current`。
- 变更检测比较被追踪属性（邮编前缀、城市、州）的 SHA-256 哈希。
- 整份快照用一次 MERGE 完成，采用经典的"变更行放两遍"技巧：`merge_key = seller_id` 的那行关闭旧版本；`merge_key = NULL` 的那行永远匹配不上，被插入为新版本。
- 首次观察到的版本从 `1900-01-01` 开始生效。第一份快照之前没有历史，与其让订单关联不上任何卖家，不如假设"首次观察到的属性在此之前一直有效"。
- 快照中缺失的卖家保持为当前版本。从快照中消失不代表删除。
- 如果一个微批次里包含多天的快照（例如故障恢复后），按日期顺序依次应用。
- 事实表按 `valid_from <= 下单日期 < valid_to` 关联。

## 考虑过的备选方案
- **SCD1（直接覆盖）：** 丢失了地区指标需要的历史。
- **Lakeflow 的 `AUTO CDC ... STORED AS SCD TYPE 2`：** 正确且简洁，但会把本项目想展示的机制本身隐藏起来（ADR-0008）。

## 影响
- 重复应用同一份快照不会产生任何变化，因为哈希相同。
- 测试：`test_scd2_keeps_history_and_is_idempotent`、`test_fact_uses_the_seller_version_valid_on_the_order_date`，以及验证"关闭的版本数等于注入的迁址数"的端到端测试。
