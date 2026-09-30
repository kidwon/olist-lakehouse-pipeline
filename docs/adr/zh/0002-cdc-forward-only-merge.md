# ADR-0002：订单 CDC 按 `change_seq` 只前进地应用

[日本語](../ja/0002-cdc-forward-only-merge.md) | [English](../en/0002-cdc-forward-only-merge.md) | **中文**

**状态：** 已采纳

## 背景
订单的每次状态变化都作为单独一行（变更后镜像）到达。到达顺序没有保证，`approved` 可能比 `shipped` 晚两天才到；同一个变更也可能到达两次。文件内的顺序和到达时间都无法说明哪条变更最新。

## 决策
- 在一个微批次内，每个 `order_id` 只保留 `change_seq` 最大的那一行。
- 以 `WHEN MATCHED AND s.change_seq > t.change_seq THEN UPDATE` 的条件 MERGE 进 `silver.orders`。序号小于或等于现有记录的变更会被忽略。
- 校验失败的变更行（例如送达时间早于下单时间）进入隔离区，订单保持上一个有效状态。

## 考虑过的备选方案
- **按 `change_ts` 排序：** 时间戳可能相同，不同系统之间的时钟也可能有偏差。唯一可靠的顺序是上游提供的序号。
- **在 Silver 保留完整变更日志，用视图推导最新状态：** 对审计有用，但变更历史在 Bronze 里已经有了。Silver 的职责是保存当前状态。
- **Lakeflow 的 `AUTO CDC ... SEQUENCE BY change_seq`：** 一行就能表达同样的语义。本项目为什么手写，见 ADR-0008。

## 影响
- 状态不会倒退。测试：`test_late_older_change_does_not_regress_status`，以及把 Silver 与从回放 staging 数据算出的标准答案逐条比对的端到端测试。
- 如果上游重置了 `change_seq`，这个方案就会失效，那属于契约变更。
