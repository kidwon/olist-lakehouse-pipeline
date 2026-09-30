# ADR-0001：幂等摄取与去重

[日本語](../ja/0001-idempotent-ingestion-and-dedup.md) | [English](../en/0001-idempotent-ingestion-and-dedup.md) | **中文**

**状态：** 已采纳

## 背景
订单明细是只追加的数据源，但上游会重复发送同一条明细：有时同一次投递里出现两遍，有时第二天又发一遍。作业也可能被重试。只要有一条重复数据进入事实表，销售额就会被重复计算。

## 决策
重复数据分两个层面去除，每个层面处理不同的情况。

1. **文件层面：** Bronze 用 Auto Loader（本地用文件流数据源）读取，配合 `trigger(availableNow=True)` 和 checkpoint。每个文件只会被摄取一次，所以作业重试时读不到新数据。
2. **行层面，批次内：** `keep_first` 按业务键 `(order_id, order_item_id)` 用 `row_number()` 只保留一行。
3. **行层面，跨批次：** Silver 的 MERGE 只有 `WHEN NOT MATCHED THEN INSERT`。已经存在的键视为"重复投递"，不当作更新处理。

两类行层面的去重数量分别以 `duplicate_in_batch` 和 `duplicate_already_loaded`（严重级别 `info`）写入 `ops.dq_metrics`，让重复数据可见，而不是被悄悄吸收掉。

## 考虑过的备选方案
- **在流上用 `dropDuplicates` 加 watermark：** 需要给"重复数据最晚多久到达"设一个上限，还要一直保留状态。对表做 MERGE 没有时间上限。
- **每次都用 Bronze 全量覆盖 Silver：** 简单，但成本随历史数据量增长，而且每次重试都要重新处理全部数据。

## 影响
- 要修正一条明细，需要新的键或者一个明确的更新数据源。这和数据源的契约（"行只会追加"）一致。
- 测试：`test_redelivered_item_is_not_counted_twice`，以及验证"Silver 行数 = 投递行数 − 重复 − 隔离"的端到端测试。
