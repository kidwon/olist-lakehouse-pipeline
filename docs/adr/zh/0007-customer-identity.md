# ADR-0007：用 `customer_unique_id` 识别客户

[日本語](../ja/0007-customer-identity.md) | [English](../en/0007-customer-identity.md) | **中文**

**状态：** 已采纳

## 背景
在 Olist 中，`customer_id` 是**每个订单**生成一个的，代表真实个人的是 `customer_unique_id`。按 `customer_id` 计数实际上是在数订单，所有复购相关的指标都会变成零。

## 决策
- Silver 按原样保留 `customer_id`，因为订单引用的是它。
- `gold.dim_customer` 每个 `customer_unique_id` 一行，属性取自此人最近一次订单，同时带上 `order_count` 和 `first_order_ts`。
- 事实表带有通过订单的客户记录解析出的 `customer_unique_id`。

## 影响
- 真实数据中，82,406 个 `customer_id` 对应 79,682 个人。
- 测试：`test_dim_customer_counts_people_not_customer_ids`。
