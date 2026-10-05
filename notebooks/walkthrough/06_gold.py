# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Gold: star schema, customer identity and the mart / 星型模型、客户身份与 mart / スタースキーマ、顧客の名寄せ、マート
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC Gold is what analysts query: `fact_order_item` (grain: one order item) with `dim_customer`, `dim_seller` (SCD2, notebook 04), `dim_product`, `dim_date`, and one mart, `mart_seller_delivery_performance`. This notebook runs the builders in `src/olist_pipeline/gold.py` on small inputs and shows three decisions: who a customer is, how the fact updates in place, and what counts as GMV. Design decisions: ADR-0004 and ADR-0007.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC Gold 是分析人员查询的层：`fact_order_item`（粒度：订单明细）加上 `dim_customer`、`dim_seller`（SCD2，见 notebook 04）、`dim_product`、`dim_date`，以及一张 mart `mart_seller_delivery_performance`。这个 notebook 在小数据上运行 `src/olist_pipeline/gold.py` 的构建函数，展示三个决策：客户是谁、事实表怎么原地更新、什么算 GMV。设计决策见 ADR-0004 和 ADR-0007。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC Gold はアナリストがクエリする層です。`fact_order_item`（粒度：注文明細）に、`dim_customer`、`dim_seller`（SCD2、ノートブック 04）、`dim_product`、`dim_date`、そしてマート `mart_seller_delivery_performance` が加わります。このノートブックでは `src/olist_pipeline/gold.py` の構築関数を小さなデータで実行し、3つの判断を示します。顧客とは誰か、ファクトをどう更新するか、何を GMV とみなすかです。設計判断は ADR-0004 と ADR-0007 を参照してください。

# COMMAND ----------

import datetime as dt
from decimal import Decimal as D

from pyspark.sql import functions as F

from walkthrough_setup import fresh_table, setup
from olist_pipeline import gold

spark, SCHEMA = setup()
show = globals().get("display") or (lambda df: df.show(truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Customers are people, not `customer_id`s / 客户是人，不是 `customer_id` / 顧客は人であって `customer_id` ではない
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC Olist issues a **new `customer_id` for every order**. The real person is `customer_unique_id`. Count `customer_id`s and you are counting orders, and every repeat-customer metric comes out as zero. `build_dim_customer` builds one row per person, taking the address from their most recent order. In the real data: 82,406 `customer_id`s → 79,682 people.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC Olist **每下一个订单就生成一个新的 `customer_id`**，代表真实个人的是 `customer_unique_id`。按 `customer_id` 计数，实际上是在数订单，所有复购指标都会是零。`build_dim_customer` 每个人一行，地址取自此人最近一次订单。真实数据中：82,406 个 `customer_id` → 79,682 人。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC Olist は**注文のたびに新しい `customer_id` を発行**します。実在の人物を表すのは `customer_unique_id` です。`customer_id` を数えると注文を数えることになり、リピート顧客の指標はすべてゼロになります。`build_dim_customer` は1人1行で、住所はその人の直近の注文から取ります。実データでは 82,406 件の `customer_id` が 79,682 人になります。

# COMMAND ----------

customers = spark.createDataFrame(
    [("c1", "ana", "01001", "sao paulo", "SP"), ("c2", "ana", "80010", "curitiba", "PR"), ("c3", "bruno", "20001", "rio de janeiro", "RJ")],
    "customer_id string, customer_unique_id string, customer_zip_code_prefix string, customer_city string, customer_state string",
)
orders = spark.createDataFrame(
    [("o1", "c1", "delivered", dt.datetime(2018, 1, 10), dt.datetime(2018, 1, 15), dt.datetime(2018, 1, 20)),
     ("o2", "c2", "delivered", dt.datetime(2018, 5, 2), dt.datetime(2018, 5, 12), dt.datetime(2018, 5, 10)),
     ("o3", "c3", "canceled", dt.datetime(2018, 5, 3), None, dt.datetime(2018, 5, 20))],
    "order_id string, customer_id string, order_status string, order_purchase_ts timestamp, "
    "order_delivered_customer_ts timestamp, order_estimated_delivery_ts timestamp",
)
print(f"customer_id values: {customers.count()}")
show(gold.build_dim_customer(customers, orders))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The fact table: grain, flags and in-place updates / 事实表：粒度、标志与原地更新 / ファクト：粒度、フラグ、その場での更新
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC `build_fact_order_item` joins items to their order, customer and the seller version valid on the order date, and derives `is_cancelled`, `is_delivered` and `is_on_time` (delivered on or before the estimated date). Each row is dated by **the sale** (`order_date_key`), never by when the file arrived, so a late item lands on its original day. `arrival_lag_days` keeps the lateness visible.
# MAGIC
# MAGIC `merge_fact` then MERGEs on the grain and rewrites a row **only when its content hash changed**. Below, the fact is loaded once; then order `o3`'s item, which arrived 2 days late, is added and nothing else changes. The Delta history shows the second MERGE inserted 1 row and updated 0.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC `build_fact_order_item` 把明细关联到订单、客户，以及下单当天有效的卖家版本，并计算 `is_cancelled`、`is_delivered` 和 `is_on_time`（在预计日期当天或之前送达）。每一行都按**销售发生日**（`order_date_key`）归日，而不是按文件到达的日期，所以迟到的明细会落在它原本的那一天。`arrival_lag_days` 保留了迟到的信息。
# MAGIC
# MAGIC 之后 `merge_fact` 按粒度做 MERGE，**只有内容哈希变了**才重写这一行。下面先加载一次事实表；然后加入迟到 2 天的 `o3` 明细，其他行都不变。Delta 历史显示第二次 MERGE 插入了 1 行、更新了 0 行。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC `build_fact_order_item` は明細を注文、顧客、注文日に有効だったセラーのバージョンに結合し、`is_cancelled`、`is_delivered`、`is_on_time`（予定日当日またはそれより前に配達）を導出します。各行は、ファイルが届いた日ではなく**販売日**（`order_date_key`）で計上されるため、遅れて届いた明細も本来の日に入ります。遅延の情報は `arrival_lag_days` に残ります。
# MAGIC
# MAGIC その後 `merge_fact` が粒度キーで MERGE し、**内容のハッシュが変わったときだけ**行を書き直します。以下では、まずファクトを一度ロードし、次に2日遅れで届いた `o3` の明細を追加します。他の行は何も変わりません。Delta の履歴を見ると、2回目の MERGE は1行を挿入し、0行を更新しています。

# COMMAND ----------

item_schema = "order_id string, order_item_id int, product_id string, seller_id string, order_purchase_ts timestamp, " \
              "price decimal(12,2), freight_value decimal(12,2), arrival_lag_days int"
day_items = spark.createDataFrame(
    [("o1", 1, "p1", "s1", dt.datetime(2018, 1, 10), D("120"), D("15"), 0), ("o2", 1, "p2", "s1", dt.datetime(2018, 5, 2), D("80"), D("10"), 0)], item_schema
)
late_item = spark.createDataFrame([("o3", 1, "p3", "s1", dt.datetime(2018, 5, 3), D("50"), D("8"), 2)], item_schema)
sellers = spark.createDataFrame([("s1", 111, dt.date(1900, 1, 1), None)], "seller_id string, seller_sk long, valid_from date, valid_to date")

FACT = fresh_table(spark, SCHEMA, "demo_fact_order_item")
gold.merge_fact(spark, FACT, gold.build_fact_order_item(day_items, orders, customers, sellers))
gold.merge_fact(spark, FACT, gold.build_fact_order_item(day_items.unionByName(late_item), orders, customers, sellers))

show(spark.table(FACT).select("order_id", "order_date_key", "customer_unique_id", "order_status", "price",
                              "is_cancelled", "is_delivered", "is_on_time", "arrival_lag_days").orderBy("order_id"))
show(spark.sql(f"DESCRIBE HISTORY {FACT}").where("operation = 'MERGE'")
     .select("version", F.col("operationMetrics.numTargetRowsInserted").alias("inserted"),
             F.col("operationMetrics.numTargetRowsUpdated").alias("updated")).orderBy("version"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The mart: what counts as GMV / mart：什么算 GMV / マート：何を GMV とみなすか
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC `build_mart_seller_delivery` aggregates per seller and month. Definitions matter more than code here: **GMV excludes cancelled and unavailable orders**, the on-time rate is on-time ÷ **delivered** orders (not all orders), and the review score is averaged per order first so an order with three items does not count three times. Order `o3` is cancelled, so it is not in GMV.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC `build_mart_seller_delivery` 按卖家和月份汇总。这里口径定义比代码更重要：**GMV 不包括已取消和缺货的订单**；准时率是准时订单 ÷ **已送达**订单（不是全部订单）；评分先按订单取平均，避免一个有三件商品的订单被算三次。订单 `o3` 已取消，所以不计入 GMV。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC `build_mart_seller_delivery` はセラー × 月で集計します。ここではコードより定義が重要です。**GMV にはキャンセル・在庫切れの注文を含めません**。定時配達率は定時配達の注文 ÷ **配達済み**の注文（全注文ではありません）です。レビュー評価はまず注文ごとに平均するため、3点の商品を含む注文が3回数えられることはありません。注文 `o3` はキャンセル済みなので GMV に入りません。

# COMMAND ----------

reviews = spark.createDataFrame([("o1", 5), ("o2", 2)], "order_id string, review_score int")
dim_seller = spark.createDataFrame([("s1", "sao paulo", "SP", True)], "seller_id string, seller_city string, seller_state string, is_current boolean")
show(gold.build_mart_seller_delivery(spark.table(FACT), reviews, dim_seller).orderBy("year_month"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try it yourself / 自己试试 / やってみよう
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC 1. Change `o2`'s status from `delivered` to `canceled` in `orders` and rerun section 2. How many fact rows does the MERGE update, and what happens to May's GMV in section 3?
# MAGIC 2. `o2` was delivered on 05-12 but estimated for 05-10. Change the estimate to 05-12. Is it on time now? Why does the code compare dates, not timestamps?
# MAGIC 3. Give `ana` a third order. Which address does `dim_customer` show, and is that the right business choice?
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 1. 把 `orders` 里 `o2` 的状态从 `delivered` 改成 `canceled`，重跑第 2 节。MERGE 更新了几行事实数据？第 3 节里 5 月的 GMV 会怎样？
# MAGIC 2. `o2` 在 05-12 送达，但预计日期是 05-10。把预计日期改成 05-12，现在算准时吗？为什么代码比较的是日期而不是时间戳？
# MAGIC 3. 给 `ana` 再加一个订单。`dim_customer` 显示的是哪个地址？从业务上说这个选择对吗？
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 1. `orders` の `o2` のステータスを `delivered` から `canceled` に変えて2節を再実行してください。MERGE で更新されるファクトの行はいくつでしょうか。3節の5月の GMV はどうなりますか。
# MAGIC 2. `o2` は 05-12 に配達されましたが、予定日は 05-10 でした。予定日を 05-12 に変えると定時配達になりますか。コードがタイムスタンプではなく日付で比較しているのはなぜでしょうか。
# MAGIC 3. `ana` に3件目の注文を追加してください。`dim_customer` にはどの住所が表示されますか。それは業務上正しい選択でしょうか。

# COMMAND ----------

# MAGIC %md
# MAGIC ## In the interview / 面试时怎么说 / 面接では
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC > "Gold is a star schema at the order-item grain. Two modelling details matter. Olist creates a new `customer_id` per order, so the customer dimension is keyed on `customer_unique_id`: 82,406 IDs are really 79,682 people. And every item is dated by the sale, not the file, so a late item lands on its original day; the fact is MERGEd on its grain and only rows whose content hash changed are rewritten. In the mart, GMV excludes cancelled orders and the on-time rate is over delivered orders only."
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC > "Gold 是以订单明细为粒度的星型模型，有两个建模细节很关键。Olist 每个订单都会生成一个新的 `customer_id`，所以客户维度用 `customer_unique_id` 作键：82,406 个 ID 实际上是 79,682 个人。另外，每条明细按销售发生日归日，而不是按文件到达日，所以迟到的明细会落在原本的那一天；事实表按粒度 MERGE，只重写内容哈希有变化的行。在 mart 里，GMV 不含已取消的订单，准时率只按已送达的订单计算。"
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC > 「Gold は注文明細を粒度とするスタースキーマで、モデリング上の重要な点が2つあります。Olist は注文ごとに新しい `customer_id` を発行するため、顧客ディメンションは `customer_unique_id` をキーにしています。82,406 件の ID は実際には 79,682 人です。また、各明細はファイルの到着日ではなく販売日で計上するので、遅れて届いた明細も本来の日に入ります。ファクトは粒度キーで MERGE し、内容のハッシュが変わった行だけを書き直します。マートでは、GMV にキャンセルされた注文を含めず、定時配達率は配達済みの注文だけで計算しています。」
