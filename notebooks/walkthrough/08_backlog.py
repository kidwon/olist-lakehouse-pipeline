# Databricks notebook source
# MAGIC %md
# MAGIC # 08 Periodic snapshot: daily order backlog / 周期快照：每日订单积压 / 定期スナップショット：日次の注文残
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC The third of Kimball's fact table types. A **periodic snapshot** records the state of something at regular intervals: inventory at the end of each day, a balance at the end of each month. Here it is the order backlog: `gold.fact_daily_order_backlog` has one row per **day × status × customer state**, with how many orders were still open, their value, their median age, how many had been open more than 30 days and how many were already past their estimated delivery date.
# MAGIC
# MAGIC It is built from the append-only change log (`silver.order_changes`, notebook 07): replaying the log tells us each order's status on any past day. Design decision: ADR-0010.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 这是 Kimball 三种事实表里的第三种。**周期快照**按固定的时间间隔记录某样东西的状态，比如每天结束时的库存、每月月底的余额。这里记录的是订单积压：`gold.fact_daily_order_backlog` 每一行是**某一天 × 某个状态 × 某个客户所在州**，记录当时还有多少订单没完成、它们的金额、已过去天数的中位数、超过 30 天还没完成的有多少、已经过了预计送达日期的有多少。
# MAGIC
# MAGIC 它是从只追加的变更日志（`silver.order_changes`，见 notebook 07）构建的：重放这份日志，就能知道每个订单在过去任意一天处于什么状态。设计决策见 ADR-0010。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC Kimball のファクトテーブル3種類のうちの3つ目です。**定期スナップショット**は、何かの状態を一定の間隔で記録します。毎日の終わりの在庫や、毎月末の残高などです。ここでは注文残を記録します。`gold.fact_daily_order_backlog` は**日付 × ステータス × 顧客の州**ごとに1行で、まだ完了していない注文の数、その金額、経過日数の中央値、30日を超えて未完了の数、配達予定日を過ぎた数を持ちます。
# MAGIC
# MAGIC これは追記専用の変更ログ（`silver.order_changes`、ノートブック 07）から作ります。ログを再生すれば、過去の任意の日に各注文がどのステータスだったかが分かります。設計判断は ADR-0010 を参照してください。

# COMMAND ----------

import datetime as dt
from decimal import Decimal

from pyspark.sql import functions as F

from walkthrough_setup import fresh_table, setup
from olist_pipeline import gold

spark, SCHEMA = setup()
show = globals().get("display") or (lambda df: df.show(truncate=False))

T = dt.datetime
CHANGE_SCHEMA = (
    "order_id string, customer_id string, order_status string, order_purchase_ts timestamp, "
    "order_estimated_delivery_ts timestamp, change_seq int, change_ts timestamp"
)


def ch(order_id, customer, status, seq, ts, purchase, estimated=T(2018, 6, 6)):
    return (order_id, customer, status, purchase, estimated, seq, ts)


P1, P2, P3 = T(2018, 6, 1, 10), T(2018, 6, 2, 9), T(2018, 5, 1, 9)
changes = [
    # o1 (SP): approved on 06-01, shipped 06-03, delivered 06-07
    ch("o1", "c1", "created", 1, P1, P1), ch("o1", "c1", "approved", 2, T(2018, 6, 1, 11), P1),
    ch("o1", "c1", "shipped", 3, T(2018, 6, 3, 9), P1), ch("o1", "c1", "delivered", 4, T(2018, 6, 7, 15), P1),
    # o2 (RJ): approved 06-02, cancelled 06-04
    ch("o2", "c2", "created", 1, P2, P2), ch("o2", "c2", "approved", 2, T(2018, 6, 2, 10), P2),
    ch("o2", "c2", "canceled", 3, T(2018, 6, 4, 12), P2),
    # o3 (SP): stuck in "processing" since May, like 300 real Olist orders
    ch("o3", "c3", "created", 1, P3, P3, T(2018, 5, 20)), ch("o3", "c3", "processing", 2, T(2018, 5, 1, 12), P3, T(2018, 5, 20)),
]
items = spark.createDataFrame([("o1", Decimal("100")), ("o2", Decimal("40")), ("o3", Decimal("75"))], "order_id string, price decimal(12,2)")
customers = spark.createDataFrame([("c1", "SP"), ("c2", "RJ"), ("c3", "SP")], "customer_id string, customer_state string")


def backlog(rows):
    return gold.build_fact_daily_order_backlog(spark.createDataFrame(rows, CHANGE_SCHEMA), items, customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Replaying the log into days / 把日志重放成每一天 / ログを日ごとに再生する
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC For every day from the purchase to the last day in the log, an order is counted under the status of its **last change on or before that day**. Two changes on the same day (created and approved on 06-01) count once, as the later one. A delivered or cancelled order leaves the backlog. Below: the first week of June, only the days that had open orders.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 从下单日到日志里的最后一天，每一天都按订单**当天结束前最后一条变更**的状态来计数。同一天的两条变更（06-01 的"已创建"和"已审核"）只算一次，按后一条算。已送达或已取消的订单会离开积压。下面是 6 月第一周、有未完成订单的日子。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 購入日からログの最終日まで、各日について、注文は**その日の終わりまでの最後の変更**のステータスで数えます。同じ日の2つの変更（06-01 の「作成」と「承認」）は1回だけ、後のほうとして数えます。配達済みやキャンセル済みの注文は注文残から外れます。以下は6月の最初の1週間で、未完了の注文があった日です。

# COMMAND ----------

snapshot = backlog(changes)
show(snapshot.where("snapshot_date BETWEEN DATE'2018-06-01' AND DATE'2018-06-07'")
     .select("snapshot_date", "order_status", "customer_state", "open_orders", "open_order_value",
             "median_age_days", "orders_open_over_30_days", "overdue_orders")
     .orderBy("snapshot_date", "order_status", "customer_state"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The stuck order stays visible / 卡住的订单一直可见 / 止まった注文は見え続ける
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC `o3` has been "processing" since May 1st. It is never delivered, so it stays in every day's backlog, its age grows, and from day 31 it counts in `orders_open_over_30_days`; after May 20th it is also overdue. Dropping such orders would hide a real problem, so they are kept and made measurable instead. Medians, not averages, keep a few very old orders from distorting the age.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC `o3` 从 5 月 1 日起就一直"处理中"。它永远不会送达，所以每天都留在积压里，年龄越来越大，从第 31 天起计入 `orders_open_over_30_days`；5 月 20 日之后它也算逾期。把这样的订单去掉会掩盖真实的问题，所以把它们保留下来，并让它们可以被衡量。用中位数而不用平均数，是为了避免少数特别久的订单扭曲年龄。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC `o3` は5月1日から「処理中」のままです。配達されることはないため毎日の注文残に残り続け、経過日数が伸び、31日目からは `orders_open_over_30_days` に数えられます。5月20日以降は期限切れ（overdue）にもなります。こうした注文を除外すると実際の問題が隠れてしまうため、残したうえで測れるようにしています。平均ではなく中央値を使うのは、ごく一部の非常に古い注文で経過日数が歪まないようにするためです。

# COMMAND ----------

show(snapshot.where("order_status = 'processing' AND snapshot_date IN (DATE'2018-05-01', DATE'2018-05-31', DATE'2018-06-01', DATE'2018-06-07')")
     .select("snapshot_date", "open_orders", "median_age_days", "orders_open_over_30_days", "overdue_orders").orderBy("snapshot_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. A late change restates the past / 迟到的变更修正过去 / 遅れて届いた変更が過去を修正する
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC The snapshot is computed by **event time**: the day a change happened, not the day it arrived. So when `o1`'s `shipped` change (dated 06-03) arrives late, the days already published (06-03 to 06-06) are wrong and must be corrected. That is the deliberate departure from Kimball's classic periodic snapshot, where a period is written once and never changed. With this data the classic rule is not an option: 98% of the history arrived in the single backfill batch, so "as known on the arrival day" would pile two years onto one day.
# MAGIC
# MAGIC `merge_fact(..., delete_missing=True)` applies the correction: rows whose numbers changed are updated, new combinations are inserted, and combinations that no longer exist (`o1` "approved" on 06-03..06-06) are **deleted**. The Delta history shows exactly what was restated.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 这张快照按**事件时间**计算：变更发生的那一天，而不是到达的那一天。所以当 `o1` 的 `shipped` 变更（发生在 06-03）迟到时，已经发布的那几天（06-03 到 06-06）就是错的，必须修正。这是有意偏离 Kimball 经典周期快照"每期写一次、之后不再改"的地方。在这份数据上，经典规则行不通：98% 的历史都在唯一一次回填批次里到达，按"到达当天已知的情况"会把两年的历史堆到一天上。
# MAGIC
# MAGIC `merge_fact(..., delete_missing=True)` 负责完成修正：数字变了的行更新，新的组合插入，不再存在的组合（06-03 到 06-06 的 `o1` "已审核"）**删除**。Delta 的历史记录能准确看到修正了什么。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC このスナップショットは**イベント時刻**、つまり変更が届いた日ではなく起きた日で計算します。そのため、`o1` の `shipped` の変更（06-03 に発生）が遅れて届くと、既に公開した日（06-03〜06-06）の数字は誤りになり、修正が必要です。これは、各期間を一度だけ書いて以後変更しないという Kimball の古典的な定期スナップショットから、意図的に外れている点です。このデータでは古典的なルールは使えません。履歴の98%が1回のバックフィルで届いているため、「届いた日の時点で分かっていたこと」で数えると、2年分の履歴が1日に積み上がってしまいます。
# MAGIC
# MAGIC `merge_fact(..., delete_missing=True)` が修正を反映します。数字が変わった行は更新し、新しい組み合わせは挿入し、存在しなくなった組み合わせ（06-03〜06-06 の `o1` の「承認」）は**削除**します。何が修正されたかは Delta の履歴で正確に分かります。

# COMMAND ----------

BACKLOG = fresh_table(spark, SCHEMA, "demo_fact_daily_order_backlog")
before_late = [c for c in changes if not (c[0] == "o1" and c[2] == "shipped")]
gold.merge_fact(spark, BACKLOG, backlog(before_late), gold.BACKLOG_KEYS, delete_missing=True)
print("before the late change, SP on 06-04:")
show(spark.table(BACKLOG).where("snapshot_date = DATE'2018-06-04' AND customer_state = 'SP'").select("order_status", "open_orders"))

gold.merge_fact(spark, BACKLOG, backlog(changes), gold.BACKLOG_KEYS, delete_missing=True)
print("after the late 'shipped' change arrived, SP on 06-04:")
show(spark.table(BACKLOG).where("snapshot_date = DATE'2018-06-04' AND customer_state = 'SP'").select("order_status", "open_orders"))
show(spark.sql(f"DESCRIBE HISTORY {BACKLOG}").where("operation = 'MERGE'")
     .select("version", F.col("operationMetrics.numTargetRowsInserted").alias("inserted"),
             F.col("operationMetrics.numTargetRowsUpdated").alias("updated"),
             F.col("operationMetrics.numTargetRowsDeleted").alias("deleted")).orderBy("version"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try it yourself / 自己试试 / やってみよう
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC 1. Move `o2`'s cancellation to 06-02. Which day disappears from the RJ backlog, and what does the MERGE report?
# MAGIC 2. Give `o3` a customer that is not in `customers`. Which `customer_state` does it get, and why must that never be NULL?
# MAGIC 3. Sum `open_orders` over all days for `o1`. Why is it the number of days it was open, not 1? What does that tell you about adding up a snapshot across time?
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 1. 把 `o2` 的取消改到 06-02。RJ 的积压里少了哪一天？MERGE 报告了什么？
# MAGIC 2. 给 `o3` 一个不在 `customers` 里的客户。它的 `customer_state` 会是什么？为什么它绝不能是 NULL？
# MAGIC 3. 把 `o1` 所有日子的 `open_orders` 加起来。为什么结果是它未完成的天数，而不是 1？这说明快照的数字能不能跨时间相加？
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 1. `o2` のキャンセルを 06-02 に移してください。RJ の注文残からどの日が消え、MERGE は何を報告しますか。
# MAGIC 2. `o3` に `customers` にない顧客を割り当ててください。`customer_state` は何になりますか。それが決して NULL であってはならないのはなぜでしょうか。
# MAGIC 3. `o1` の全日分の `open_orders` を合計してください。結果が1ではなく未完了だった日数になるのはなぜでしょうか。スナップショットの値を時間方向に足し合わせてよいかについて、何が分かりますか。

# COMMAND ----------

# MAGIC %md
# MAGIC ## In the interview / 面试时怎么说 / 面接では
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC > "With the append-only change log in place, I added the third Kimball fact type, a periodic snapshot of the daily order backlog by status and customer state: open orders, their value, median age, orders open over 30 days and overdue orders. It is computed by event time, which departs from the classic 'write once per period' rule on purpose: 98% of the history arrived in one backfill batch, and late changes should correct the days they belong to. The snapshot is MERGEd on its grain with deletes for combinations that disappear, so every restatement is visible in the Delta history. Snapshot measures are semi-additive: you can sum across states, never across days."
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC > "有了只追加的变更日志之后，我补上了 Kimball 的第三种事实表：按状态和客户所在州统计的每日订单积压周期快照，记录未完成订单数、金额、年龄中位数、超过 30 天的订单数和逾期订单数。它按事件时间计算，有意偏离了经典的'每期只写一次'规则：98% 的历史都在一次回填批次里到达，而迟到的变更应该修正它所属的那一天。快照按粒度做 MERGE，不再存在的组合会被删除，所以每一次修正在 Delta 历史里都看得到。快照的数字是半可加的：可以跨州相加，但绝不能跨日期相加。"
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC > 「追記専用の変更ログができたので、Kimball の3つ目のファクトとして、ステータスと顧客の州ごとの日次注文残の定期スナップショットを追加しました。未完了の注文数、金額、経過日数の中央値、30日超の注文数、期限切れの注文数を持ちます。イベント時刻で計算しており、『期間ごとに一度だけ書く』という古典的なルールからは意図的に外れています。履歴の98%が1回のバックフィルで届いており、遅れて届いた変更は本来の日を修正すべきだからです。スナップショットは粒度キーで MERGE し、存在しなくなった組み合わせは削除するので、すべての修正が Delta の履歴に残ります。スナップショットの値は半加算的で、州をまたいで足すことはできても、日付をまたいで足すことはできません。」
