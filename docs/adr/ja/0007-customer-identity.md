# ADR-0007: 顧客は `customer_unique_id` で識別する

**日本語** | [English](../en/0007-customer-identity.md) | [中文](../zh/0007-customer-identity.md)

**ステータス:** 採用

## 背景
Olist では `customer_id` が**注文ごと**に発行されます。実在の人物を表すのは `customer_unique_id` です。`customer_id` を数えると注文数を数えることになり、リピート顧客に関する指標はすべてゼロになります。

## 決定
- Silver では、注文から参照されるため、`customer_id` を届いたとおりに保持します。
- `gold.dim_customer` は `customer_unique_id` ごとに1行とし、属性は直近の注文から取得します。あわせて `order_count` と `first_order_ts` を持たせます。
- ファクトテーブルには、注文の顧客レコードを経由して解決した `customer_unique_id` を持たせます。

## 影響
- 実データでは、82,406 件の `customer_id` が 79,682 人に集約されます。
- テスト: `test_dim_customer_counts_people_not_customer_ids`。
