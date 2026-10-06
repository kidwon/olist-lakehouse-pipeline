# Databricks notebook source
# MAGIC %md
# MAGIC # 07 Accumulating snapshot: order fulfillment / 累积快照：订单履约 / 累積スナップショット：注文フルフィルメント
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC Kimball names three kinds of fact table. A **transaction** fact has one row per event (`fact_order_item`: one per order line). A **periodic snapshot** has one row per entity per period. An **accumulating snapshot** has one row per process instance that is **updated in place** as the process reaches each milestone. `gold.fact_order_fulfillment` is the third kind: one row per order, carrying purchase → approved → handed to the carrier → delivered (or cancelled / unavailable), and the hours spent in each stage. Design decision: ADR-0009.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC Kimball 把事实表分成三种。**事务事实表**每个事件一行（`fact_order_item`：每个订单明细一行）；**周期快照**每个实体每个周期一行；**累积快照**每个流程实例一行，流程每到一个阶段就**原地更新**这一行。`gold.fact_order_fulfillment` 属于第三种：每个订单一行，记录下单 → 审核 → 交给承运商 → 送达（或取消／缺货），以及每个阶段花了多少小时。设计决策见 ADR-0009。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC Kimball はファクトテーブルを3種類に分けています。**トランザクション**・ファクトはイベントごとに1行（`fact_order_item`：注文明細ごとに1行）、**定期スナップショット**はエンティティ × 期間ごとに1行、**累積スナップショット**はプロセスのインスタンスごとに1行で、マイルストーンに達するたびに**その行を更新**します。`gold.fact_order_fulfillment` は3つ目の種類で、1注文1行に、購入 → 承認 → 配送業者への引き渡し → 配達（またはキャンセル／在庫切れ）と、各段階に掛かった時間を持ちます。設計判断は ADR-0009 を参照してください。

# COMMAND ----------

import datetime as dt

from pyspark.sql import functions as F

from walkthrough_setup import fresh_table, setup
from olist_pipeline import gold, silver

spark, SCHEMA = setup()
show = globals().get("display") or (lambda df: df.show(truncate=False))

T = dt.datetime
CHANGE_SCHEMA = (
    "order_id string, customer_id string, order_status string, order_purchase_ts timestamp, order_approved_ts timestamp, "
    "order_delivered_carrier_ts timestamp, order_delivered_customer_ts timestamp, order_estimated_delivery_ts timestamp, "
    "change_seq int, change_ts timestamp"
)
E = T(2018, 6, 12)  # estimated delivery date for every demo order
# Each tuple is one CDC change (an after-image): the order's timestamps as known at that change.
all_changes = [
    # o1: a normal order, delivered on day 5
    ("o1", "c1", "created",   T(2018, 6, 1, 10), None,              None,              None,              E, 1, T(2018, 6, 1, 10)),
    ("o1", "c1", "approved",  T(2018, 6, 1, 10), T(2018, 6, 1, 11), None,              None,              E, 2, T(2018, 6, 1, 11)),
    ("o1", "c1", "shipped",   T(2018, 6, 1, 10), T(2018, 6, 1, 11), T(2018, 6, 2, 15), None,              E, 3, T(2018, 6, 2, 15)),
    ("o1", "c1", "delivered", T(2018, 6, 1, 10), T(2018, 6, 1, 11), T(2018, 6, 2, 15), T(2018, 6, 6, 15), E, 4, T(2018, 6, 6, 15)),
    # o2: approved, then cancelled
    ("o2", "c2", "created",   T(2018, 6, 1, 12), None,              None,              None,              E, 1, T(2018, 6, 1, 12)),
    ("o2", "c2", "approved",  T(2018, 6, 1, 12), T(2018, 6, 1, 13), None,              None,              E, 2, T(2018, 6, 1, 13)),
    ("o2", "c2", "canceled",  T(2018, 6, 1, 12), T(2018, 6, 1, 13), None,              None,              E, 3, T(2018, 6, 2, 9)),
    # o3: like 559 real Olist orders, handed to the carrier BEFORE payment was approved
    ("o3", "c3", "created",   T(2018, 6, 3, 9),  None,              None,              None,              E, 1, T(2018, 6, 3, 9)),
    ("o3", "c3", "shipped",   T(2018, 6, 3, 9),  None,              T(2018, 6, 3, 14), None,              E, 2, T(2018, 6, 3, 14)),
    ("o3", "c3", "approved",  T(2018, 6, 3, 9),  T(2018, 6, 3, 20), T(2018, 6, 3, 14), None,              E, 3, T(2018, 6, 3, 20)),
    # o4: still on its way
    ("o4", "c4", "created",   T(2018, 6, 5, 8),  None,              None,              None,              E, 1, T(2018, 6, 5, 8)),
    ("o4", "c4", "approved",  T(2018, 6, 5, 8),  T(2018, 6, 5, 9),  None,              None,              E, 2, T(2018, 6, 5, 9)),
]
items = spark.createDataFrame([("o1", 120.0), ("o1", 30.0), ("o2", 80.0), ("o3", 45.0), ("o4", 60.0)], "order_id string, price double")
customers = spark.createDataFrame([("c1", "ana"), ("c2", "bruno"), ("c3", "carla"), ("c4", "ana")], "customer_id string, customer_unique_id string")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The change log behind it / 背后的变更日志 / 背後にある変更ログ
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC `silver.orders` keeps only each order's **current** state; when it was cancelled, or how it got there, is gone. So the fact is built from `silver.order_changes`, an **append-only** log of every validated change, keyed `(order_id, change_seq)`. Re-sent changes are ignored and nothing is ever updated: once a state transition is written, it stays. In the pipeline it is its own stream with its own checkpoint, so a new deployment backfills the full history from bronze.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC `silver.orders` 只保存每个订单的**当前**状态；什么时候取消的、怎么走到现在的，都已经看不到了。所以事实表是从 `silver.order_changes` 构建的。它是一张**只追加**的日志，保存每一条校验通过的变更，键为 `(order_id, change_seq)`。重复发送的变更会被忽略，也从不更新：状态变化一旦写入就不再改变。在管道里，它是一条带独立 checkpoint 的单独数据流，新部署时会从 Bronze 回填完整历史。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC `silver.orders` は各注文の**現在**の状態しか持たず、いつキャンセルされたか、どう今に至ったかは失われています。そのため、ファクトは `silver.order_changes` から作ります。検証済みの変更をすべて保存する**追記専用**のログで、キーは `(order_id, change_seq)` です。再送された変更は無視され、更新は一切しません。一度書かれた状態遷移はそのまま残ります。パイプラインでは独自のチェックポイントを持つ別のストリームなので、新しくデプロイしても Bronze から全履歴をバックフィルします。

# COMMAND ----------

LOG = fresh_table(spark, SCHEMA, "demo_order_changes")
log_rows = spark.createDataFrame(all_changes, CHANGE_SCHEMA)
silver.merge_insert_new(spark, LOG, log_rows, ["order_id", "change_seq"])
silver.merge_insert_new(spark, LOG, log_rows, ["order_id", "change_seq"])  # the same changes re-sent
print(f"changes delivered twice: {2 * log_rows.count()}, rows in the log: {spark.table(LOG).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. One row per order / 每个订单一行 / 1注文1行
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC `gold.build_fact_order_fulfillment` turns 12 changes into 4 rows. Milestones come from each order's latest change; the end of `o2` comes from its `canceled` change. Durations are in hours. A milestone that has not happened yet gets the date key `-1`, a special "not yet happened" row in `dim_date`: following Kimball, a fact's date key is never NULL. Look at `o3`: it was handed to the carrier **before** payment was approved, so "approved → carrier" would be −6 hours. The raw timestamps stay as they are, the negative duration becomes NULL, and the order is flagged. A negative number would quietly drag every average down.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC `gold.build_fact_order_fulfillment` 把 12 条变更变成 4 行。各阶段时间取自每个订单最新的变更，`o2` 的结束时间取自它的 `canceled` 变更。时长以小时为单位。还没发生的阶段，日期键是 `-1`，指向 `dim_date` 里一行"尚未发生"的特殊记录：按照 Kimball 的做法，事实表的日期键从不为 NULL。看一下 `o3`：它在付款审核**之前**就交给了承运商，所以"审核 → 交接"会是 −6 小时。原始时间戳保持不变，负的时长变成 NULL，订单被打上标记。负数会悄悄拉低所有平均值。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC `gold.build_fact_order_fulfillment` は12件の変更を4行にします。マイルストーンは各注文の最新の変更から、`o2` の終了は `canceled` の変更から取ります。所要時間は時間単位です。まだ起きていないマイルストーンの日付キーは `-1` で、`dim_date` の「未発生」という特別な行を指します。Kimball に従い、ファクトの日付キーは決して NULL にしません。`o3` を見てください。支払いが承認される**前に**配送業者へ引き渡されているため、「承認 → 引き渡し」は −6 時間になります。元のタイムスタンプはそのまま残し、負の所要時間は NULL にして、注文にフラグを付けます。負の値があると、すべての平均値が気付かないうちに下がってしまいます。

# COMMAND ----------

fact = gold.build_fact_order_fulfillment(spark.table(LOG), items, customers)
show(
    fact.select("order_id", "customer_unique_id", "current_status", "end_reason", "hours_to_approve", "hours_to_ship",
                "hours_in_transit", "hours_total", "hours_late", "is_on_time", "has_inconsistent_milestones", "item_count", "order_value")
    .orderBy("order_id")
)
show(fact.select("order_id", "purchase_date_key", "approved_date_key", "shipped_date_key", "delivered_date_key", "estimated_date_key").orderBy("order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Updated in place / 原地更新 / その場で更新される
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC This is what makes it an *accumulating* snapshot. Below, `o4` is first loaded while it is only approved. Then it is shipped and delivered, the fact is rebuilt and MERGEd on `order_id`: the same row is rewritten and nothing is added. The Delta history shows the second MERGE updated exactly one row (`o4`); the other three did not change, so their content hash was equal and they were left alone.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 这就是它被称为*累积*快照的原因。下面先在 `o4` 只审核通过时加载一次；之后它发货、送达，重新构建事实表并按 `order_id` MERGE：重写的是同一行，不会新增行。Delta 历史显示第二次 MERGE 正好更新了一行（`o4`）；另外三行没有变化，内容哈希相同，所以没有被重写。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC これが*累積*スナップショットと呼ばれる理由です。以下では、まず `o4` が承認済みの段階で一度ロードします。その後、発送・配達されたらファクトを作り直して `order_id` で MERGE します。同じ行が書き直され、行は増えません。Delta の履歴を見ると、2回目の MERGE はちょうど1行（`o4`）だけを更新しています。他の3行は変化がなく、内容のハッシュが同じなので書き直されていません。

# COMMAND ----------

FACT = fresh_table(spark, SCHEMA, "demo_fact_order_fulfillment")
gold.merge_fact(spark, FACT, fact, ["order_id"])

later = [
    ("o4", "c4", "shipped",   T(2018, 6, 5, 8), T(2018, 6, 5, 9), T(2018, 6, 6, 10), None,               E, 3, T(2018, 6, 6, 10)),
    ("o4", "c4", "delivered", T(2018, 6, 5, 8), T(2018, 6, 5, 9), T(2018, 6, 6, 10), T(2018, 6, 13, 16), E, 4, T(2018, 6, 13, 16)),
]
silver.merge_insert_new(spark, LOG, spark.createDataFrame(later, CHANGE_SCHEMA), ["order_id", "change_seq"])
gold.merge_fact(spark, FACT, gold.build_fact_order_fulfillment(spark.table(LOG), items, customers), ["order_id"])

show(spark.table(FACT).where("order_id = 'o4'").select("order_id", "current_status", "hours_in_transit", "hours_total", "hours_late", "is_on_time"))
print(f"rows in the fact: {spark.table(FACT).count()}")
show(spark.sql(f"DESCRIBE HISTORY {FACT}").where("operation = 'MERGE'")
     .select("version", F.col("operationMetrics.numTargetRowsInserted").alias("inserted"),
             F.col("operationMetrics.numTargetRowsUpdated").alias("updated")).orderBy("version"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Reading it correctly: right-censoring / 正确解读：右删失 / 正しく読む：右側打ち切り
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC An accumulating snapshot always contains orders that have not finished. If you average "hours in transit" for the most recent month, only the orders that were **already delivered** have a value, and those are the fast ones. The slow ones are still NULL. So recent months look faster than they are. In the real data, June 2018's transit time is 58 hours against about 200 for earlier months. Either compare only complete periods, or compare orders of the same age.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 累积快照里永远有还没走完的订单。如果对最近一个月的"运输时长"取平均，只有**已经送达**的订单有值，而它们恰好是快的那批；慢的订单还是 NULL。所以最近的月份看起来比实际要快。真实数据中，2018 年 6 月的运输时长是 58 小时，而之前的月份大约是 200 小时。要么只比较已经完整的时间段，要么比较"订单年龄"相同的订单。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 累積スナップショットには、まだ完了していない注文が必ず含まれます。直近の月の「輸送時間」を平均すると、値を持つのは**既に配達された**注文だけで、それは早く届いた注文です。遅い注文はまだ NULL のままです。そのため、直近の月は実際より速く見えます。実データでは、2018年6月の輸送時間は 58 時間で、それ以前の月は約 200 時間です。比較するなら、完了した期間だけを比べるか、経過日数が同じ注文同士を比べます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try it yourself / 自己试试 / やってみよう
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC 1. Deliver `o4` at 23:00 on the estimated day (2018-06-12) instead. What are `hours_late` and `is_on_time`? Why does lateness count from the end of the estimated day?
# MAGIC 2. Give `o3`'s carrier timestamp a value after the approval. Does the flag disappear, and which durations come back?
# MAGIC 3. Add an `unavailable` change to `o4` instead of shipping it. What do `end_reason` and `ended_ts` show?
# MAGIC 4. Why does this table have no seller column, when `fact_order_item` does?
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 1. 改成在预计送达日（2018-06-12）的 23:00 送达 `o4`。`hours_late` 和 `is_on_time` 是多少？为什么迟到从预计送达日的结束时刻开始算？
# MAGIC 2. 把 `o3` 的交接时间改到审核之后。标记会消失吗？哪些时长会恢复？
# MAGIC 3. 不发货，而是给 `o4` 加一条 `unavailable` 变更。`end_reason` 和 `ended_ts` 会显示什么？
# MAGIC 4. `fact_order_item` 有卖家列，为什么这张表没有？
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 1. `o4` を配達予定日（2018-06-12）の 23:00 に配達するよう変えてください。`hours_late` と `is_on_time` はどうなりますか。遅延を予定日の終わりから数えるのはなぜでしょうか。
# MAGIC 2. `o3` の引き渡し日時を承認の後にしてください。フラグは消えますか。どの所要時間が戻ってきますか。
# MAGIC 3. 発送する代わりに、`o4` に `unavailable` の変更を追加してください。`end_reason` と `ended_ts` は何を示しますか。
# MAGIC 4. `fact_order_item` にはセラーの列があるのに、このテーブルにはないのはなぜでしょうか。

# COMMAND ----------

# MAGIC %md
# MAGIC ## In the interview / 面试时怎么说 / 面接では
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC > "Besides the transaction fact at order-line grain, I built an accumulating snapshot: one row per order with every milestone, role-playing date keys and the hours spent in each stage, updated in place by MERGE as the order moves. It is built from an append-only change log in silver, which also gives cancellation times and an audit trail. Negative durations from inconsistent source timestamps become NULL and are flagged; 0.76% of real orders, mostly handed to the carrier before payment approval. It showed that about 10 of the 13 days to delivery are spent in transit."
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC > "除了以订单明细为粒度的事务事实表，我还做了一张累积快照：每个订单一行，带着所有阶段的时间、角色扮演日期键和每个阶段花费的小时数，订单推进时用 MERGE 原地更新。它构建自 Silver 层一张只追加的变更日志，这张日志也提供了取消时间和审计记录。源数据时间戳不一致导致的负时长会被设为 NULL 并标记，真实订单里占 0.76%，大多是付款审核前就交给了承运商。分析结果显示，13 天的配送周期里大约 10 天花在运输上。"
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC > 「注文明細を粒度とするトランザクション・ファクトに加えて、累積スナップショットも作りました。1注文1行で、すべてのマイルストーン、ロールプレイング日付キー、各段階に掛かった時間を持ち、注文が進むたびに MERGE でその行を更新します。Silver の追記専用の変更ログから作っており、このログはキャンセル時刻と監査証跡も提供します。ソースのタイムスタンプの不整合による負の所要時間は NULL にしてフラグを付けます。実データでは注文の 0.76% で、ほとんどは支払い承認前に配送業者へ引き渡されたものです。分析の結果、配達までの約13日のうち約10日が輸送に掛かっていることが分かりました。」
