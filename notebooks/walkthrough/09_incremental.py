# Databricks notebook source
# MAGIC %md
# MAGIC # 09 Incremental gold with Change Data Feed / 用 CDF 增量处理 Gold / Change Data Feed による Gold の増分処理
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC Until v0.3.0, every run rebuilt the gold facts from all of silver. A daily batch changes one or two thousand rows out of ~100k, so most of that work is repeated. **Delta Change Data Feed (CDF)** records which rows changed in each table version. Gold now reads only those changes, turns them into the set of **affected orders**, and rebuilds just those orders. The last processed version of each source is stored in `ops.gold_watermarks` and moves only after the MERGE succeeded. Design decision: ADR-0011.
# MAGIC
# MAGIC Honest note: at this data size the time saved is small (serverless start-up dominates). The point is the pattern, and the share of orders rebuilt per run, which this notebook measures.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 到 v0.3.0 为止，每次运行都会用整个 Silver 层重建 Gold 事实表。而一个每日批次在约 10 万行里只改变一两千行，大部分工作都是重复的。**Delta Change Data Feed（CDF）**会记录每个表版本里哪些行变了。现在 Gold 只读取这些变化，把它们换算成**受影响的订单**，只重建这些订单。每张 Silver 表上次处理到的版本保存在 `ops.gold_watermarks`，只有 MERGE 成功后才会前进。设计决策见 ADR-0011。
# MAGIC
# MAGIC 坦白说：在这个数据量下省下的时间不多（主要是 serverless 的启动开销）。重点是这个模式，以及每次运行重建的订单比例，这个 notebook 会把它测出来。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC v0.3.0 までは、毎回の実行で Silver 全体から Gold のファクトを作り直していました。日次バッチで変わるのは約10万行のうち1,000〜2,000行なので、作業の大半は繰り返しです。**Delta Change Data Feed（CDF）**は、各テーブルバージョンでどの行が変わったかを記録します。Gold はその変更だけを読み、**影響を受けた注文**に変換して、その注文だけを作り直します。各 Silver テーブルの処理済みバージョンは `ops.gold_watermarks` に保存し、MERGE が成功したときだけ進めます。設計判断は ADR-0011 を参照してください。
# MAGIC
# MAGIC 正直に言うと、このデータ量では短縮できる時間はわずかです（サーバーレスの起動時間が大半を占めます）。重要なのはこのパターンと、各実行で作り直す注文の割合で、このノートブックではそれを計測します。

# COMMAND ----------

import datetime as dt
from decimal import Decimal

from pyspark.sql import functions as F

from walkthrough_setup import fresh_table, setup
from olist_pipeline import gold
from olist_pipeline.config import Config

spark, SCHEMA = setup()
show = globals().get("display") or (lambda df: df.show(truncate=False))
catalog, schema_name = SCHEMA.split(".") if "." in SCHEMA else (None, SCHEMA)


def cfg_for(run_id, full_refresh=False):
    return Config(base_path="/tmp/unused", catalog=catalog, run_id=run_id, full_refresh=full_refresh,
                  schemas={k: schema_name for k in ["bronze", "silver", "gold", "ops"]})


T = dt.datetime
CHANGES = "order_id string, customer_id string, order_status string, order_purchase_ts timestamp, order_approved_ts timestamp, " \
          "order_delivered_carrier_ts timestamp, order_delivered_customer_ts timestamp, order_estimated_delivery_ts timestamp, " \
          "change_seq int, change_ts timestamp"
for t in ["order_changes", "order_items", "customers", "reviews", "orders", "fact_order_fulfillment", "gold_watermarks", "gold_incremental_stats"]:
    fresh_table(spark, SCHEMA, t)

# A small silver layer: 200 orders, each created and approved on 06-01.
P, A, E = T(2018, 6, 1, 10), T(2018, 6, 1, 11), T(2018, 6, 12)
rows = []
for i in range(200):
    o = f"o{i:03d}"
    rows += [(o, f"c{i}", "created", P, None, None, None, E, 1, P), (o, f"c{i}", "approved", P, A, None, None, E, 2, A)]
spark.createDataFrame(rows, CHANGES).write.format("delta").saveAsTable(f"{SCHEMA}.order_changes")
spark.createDataFrame([(f"o{i:03d}", Decimal("50")) for i in range(200)], "order_id string, price decimal(12,2)").write.format("delta").saveAsTable(f"{SCHEMA}.order_items")
spark.createDataFrame([(f"c{i}", f"u{i}") for i in range(200)], "customer_id string, customer_unique_id string").write.format("delta").saveAsTable(f"{SCHEMA}.customers")
spark.createDataFrame([], "order_id string, review_score int").write.format("delta").saveAsTable(f"{SCHEMA}.reviews")
spark.createDataFrame([(f"o{i:03d}", f"c{i}") for i in range(200)], "order_id string, customer_id string").write.format("delta").saveAsTable(f"{SCHEMA}.orders")


def stats():
    return spark.table(f"{SCHEMA}.gold_incremental_stats").select("run_id", "mode", "reason", "processed_orders", "total_orders").orderBy("measured_at")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. First run: full / 第一次运行：全量 / 初回の実行：全件
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC There is nothing to compare against yet, so the plan is a full rebuild. Before reading, CDF is switched on for every source (changes are only recorded from that moment), and the current version of each source is fixed as the upper bound. After the MERGE, those versions become the watermarks.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 还没有可以比较的基准，所以计划是全量重建。读取之前，先给每张源表打开 CDF（变化只从这一刻开始记录），并把每张源表的当前版本固定为这次读取的上限。MERGE 完成后，这些版本就成为进度记录。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC まだ比較の基準がないため、計画は全件の再構築になります。読む前に、すべてのソースで CDF をオンにし（変更はこの時点からしか記録されません）、各ソースの現在のバージョンを今回の上限として固定します。MERGE の後、そのバージョンがウォーターマークになります。

# COMMAND ----------

gold.run_fact_order_fulfillment(spark, cfg_for("run-1"))
show(stats())
show(spark.table(f"{SCHEMA}.gold_watermarks").select("source", "version"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Next run: only the changed orders / 下一次运行：只处理变化的订单 / 次の実行：変わった注文だけ
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC Three orders are shipped and one customer moves. The change feed of `order_changes` gives `o001`, `o002`, `o003`; the change in `customers` maps to that customer's order `o150`. Four of 200 orders are rebuilt; the other 196 are not touched.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 有三个订单发货了，还有一个客户的信息变了。`order_changes` 的变更记录给出 `o001`、`o002`、`o003`；`customers` 的变化换算成那位客户的订单 `o150`。200 个订单里只重建 4 个，其余 196 个完全不动。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 3件の注文が発送され、1人の顧客の情報が変わります。`order_changes` の変更記録からは `o001`、`o002`、`o003` が、`customers` の変更からはその顧客の注文 `o150` が得られます。200件中4件だけを作り直し、残りの196件には一切触れません。

# COMMAND ----------

S = T(2018, 6, 2, 9)
spark.createDataFrame([(f"o00{i}", f"c{i}", "shipped", P, A, S, None, E, 3, S) for i in (1, 2, 3)], CHANGES) \
    .write.format("delta").mode("append").saveAsTable(f"{SCHEMA}.order_changes")
spark.sql(f"UPDATE {SCHEMA}.customers SET customer_unique_id = 'u150-merged' WHERE customer_id = 'c150'")

gold.run_fact_order_fulfillment(spark, cfg_for("run-2"))
show(stats())
show(spark.table(f"{SCHEMA}.fact_order_fulfillment").where("order_id IN ('o001', 'o150', 'o199')")
     .select("order_id", "customer_unique_id", "current_status", "hours_to_ship").orderBy("order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Proof: incremental equals a full rebuild / 证明：增量等于全量重建 / 証明：増分は全件再構築と同じ
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC The risk of incremental processing is silently missing a change: nothing fails, numbers are just wrong. So the result is compared with a full rebuild from silver. They must be identical. The e2e test does the same after every replay, including the day the quality gate blocks and the catch-up run after it.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 增量处理最大的风险是悄悄漏掉某个变化：什么都不会报错，只是数字错了。所以要把结果和从 Silver 全量重建的结果对比，二者必须完全一致。端到端测试在每次回放后都做同样的对比，包括被质量闸门拦下的那一天，以及之后的补跑。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 増分処理の最大のリスクは、変更を気付かないまま取りこぼすことです。何もエラーにならず、数字だけが間違います。そのため、結果を Silver からの全件再構築と比べ、完全に一致することを確認します。E2E テストでも、リプレイのたびに同じ比較をしています。品質ゲートで止まった日と、その後の追い付きの実行も含みます。

# COMMAND ----------

full = gold.build_fact_order_fulfillment(spark.table(f"{SCHEMA}.order_changes"), spark.table(f"{SCHEMA}.order_items"),
                                         spark.table(f"{SCHEMA}.customers"), spark.table(f"{SCHEMA}.reviews"))
actual = spark.table(f"{SCHEMA}.fact_order_fulfillment").select(*full.columns)
print(f"rows only in the incremental table: {actual.exceptAll(full).count()}, rows only in the full rebuild: {full.exceptAll(actual).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Escape hatch: full refresh / 应急开关：强制全量 / 非常口：強制的な全件再構築
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC Three situations rebuild everything instead: the job is started with `full_refresh=true`, a source has no watermark (for example a new source table), or the change data for the needed versions is gone (Delta keeps it only for a while). The reason is always recorded, so a full run is never a surprise.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 有三种情况会改为全量重建：作业以 `full_refresh=true` 启动；某张源表没有进度记录（比如新增了一张源表）；需要的那些版本的变更记录已经被清理了（Delta 只保留一段时间）。原因每次都会被记录下来，所以全量运行不会让人摸不着头脑。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 次の3つの場合は、代わりにすべてを作り直します。ジョブが `full_refresh=true` で起動された場合、ソースにウォーターマークがない場合（新しいソーステーブルなど）、必要なバージョンの変更データが既に消えている場合（Delta は一定期間しか保持しません）です。理由は毎回記録されるので、全件の実行が起きても原因が分からないことはありません。

# COMMAND ----------

gold.run_fact_order_fulfillment(spark, cfg_for("run-3", full_refresh=True))
show(stats())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try it yourself / 自己试试 / やってみよう
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC 1. Run section 2 again without adding changes. What does the plan say, and how many orders are processed?
# MAGIC 2. Add a review for `o010` (`INSERT INTO ... reviews`) and run again. Which source brought `o010` in?
# MAGIC 3. Why are the watermarks saved *after* the MERGE and not before? Make the MERGE fail and look at `gold_watermarks`.
# MAGIC 4. The daily backlog snapshot is not incremental. Why is "which days does a late change affect" harder than "which orders"?
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 1. 不加任何变化，再跑一次第 2 节。计划显示什么？处理了几个订单？
# MAGIC 2. 给 `o010` 加一条评价（`INSERT INTO ... reviews`），再跑一次。`o010` 是由哪张源表带进来的？
# MAGIC 3. 为什么进度要在 MERGE *之后*保存，而不是之前？让 MERGE 失败一次，再看看 `gold_watermarks`。
# MAGIC 4. 每日积压快照没有做增量。为什么"一个迟到的变更会影响哪几天"比"影响哪些订单"更难判断？
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 1. 変更を加えずに2節をもう一度実行してください。計画は何を示し、何件の注文が処理されますか。
# MAGIC 2. `o010` にレビューを追加して（`INSERT INTO ... reviews`）、もう一度実行してください。`o010` はどのソースから入ってきましたか。
# MAGIC 3. ウォーターマークを MERGE の*前*ではなく*後*に保存するのはなぜでしょうか。MERGE を一度失敗させて、`gold_watermarks` を見てください。
# MAGIC 4. 日次の注文残スナップショットは増分化していません。「遅れて届いた変更がどの日に影響するか」は、「どの注文に影響するか」より判断が難しいのはなぜでしょうか。

# COMMAND ----------

# MAGIC %md
# MAGIC ## In the interview / 面试时怎么说 / 面接では
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC > "Gold used to rebuild from all of silver every run. I turned on Change Data Feed on the silver tables; each gold fact now reads the changes since its last processed version, maps them to the affected orders — including indirect ones, like a customer change touching that customer's orders — and rebuilds only those. Watermarks per source move only after the MERGE succeeds, so a failure just reprocesses. It falls back to a full rebuild when there's no watermark, the change data has expired, or on request, and the reason is logged. The tests prove the incremental result equals a full rebuild after every replayed day. Each run now rebuilds about 2% of the orders; I'm honest that at 100k rows the wall-clock gain is small. The backlog snapshot stays a full rebuild on purpose."
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC > "Gold 以前每次都用整个 Silver 重建。我在 Silver 表上开启了 Change Data Feed，现在每张 Gold 事实表读取自上次处理版本以来的变化，把它们换算成受影响的订单（包括间接影响，比如客户信息变了会影响这位客户的订单），只重建这些订单。每张源表的进度只在 MERGE 成功后才前进，所以失败了只是重新处理。没有进度记录、变更记录已过期或者手动要求时，会退回全量重建，并记录原因。测试证明每个回放日之后，增量结果都和全量重建完全一致。现在每次只重建约 2% 的订单；但也要坦白，在 10 万行的规模下，实际节省的时间不多。每日积压快照有意保持全量重建。"
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC > 「Gold は以前、毎回 Silver 全体から作り直していました。Silver のテーブルで Change Data Feed を有効にし、今は各 Gold ファクトが前回処理したバージョン以降の変更を読み、影響を受けた注文（顧客情報の変更がその顧客の注文に及ぶような間接的な影響も含みます）に変換して、その注文だけを作り直します。ソースごとのウォーターマークは MERGE が成功したときだけ進むので、失敗しても再処理するだけです。ウォーターマークがない場合、変更データの期限が切れた場合、または要求があった場合は全件再構築に戻り、その理由を記録します。テストでは、リプレイした各日の後で、増分の結果が全件再構築と一致することを証明しています。今は1回の実行で注文の約2%だけを作り直していますが、10万行の規模では短縮できる時間は小さいことも正直にお伝えします。日次の注文残スナップショットは、意図的に全件再構築のままにしています。」
