# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Replay: turning a static dataset into a daily feed / 把静态数据集变成每日数据流 / 静的データセットを日次フィードに変える
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC Olist is a one-off Kaggle dump, but a real pipeline sees files arriving every day, with mistakes in them. `src/olist_pipeline/replay.py` bridges the gap. `prepare` turns the CSVs into feeds and gives every row a `_delivery_date` (the day it lands) and, when it was tampered with, an `_anomaly` tag. `deliver` then drops one day at a time into the landing zone.
# MAGIC
# MAGIC The tags are the **ground truth**. The pipeline never reads them; the tests compare the pipeline's own metrics against them.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC Olist 是 Kaggle 上一次性导出的数据，而真实的管道面对的是每天到达、而且带着错误的文件。`src/olist_pipeline/replay.py` 负责填补这个差距。`prepare` 把 CSV 转换成各个数据源，并给每一行打上 `_delivery_date`（哪天投递）；如果这行被动了手脚，还会打上 `_anomaly` 标签。之后 `deliver` 每次把一天的数据放进 landing 区。
# MAGIC
# MAGIC 这些标签就是**标准答案**。管道本身从不读取它们，测试会拿管道自己的指标和它们比对。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC Olist は Kaggle の一度きりのダンプですが、実際のパイプラインには誤りを含むファイルが毎日届きます。そのギャップを埋めるのが `src/olist_pipeline/replay.py` です。`prepare` は CSV を各フィードに変換し、すべての行に `_delivery_date`（届く日）を付け、手を加えた行には `_anomaly` タグも付けます。その後、`deliver` が1日分ずつランディング領域に配信します。
# MAGIC
# MAGIC これらのタグが**正解データ**です。パイプライン自体は一切読みません。テストがパイプライン自身のメトリクスとこれを突き合わせます。

# COMMAND ----------

import datetime as dt

from pyspark.sql import Window
from pyspark.sql import functions as F

from walkthrough_setup import setup
from olist_pipeline import replay
from olist_pipeline.config import Config

spark, SCHEMA = setup()
show = globals().get("display") or (lambda df: df.show(truncate=False))
cfg = Config(base_path="/tmp/unused", backfill_date=dt.date(2018, 5, 31), replay_days=10)
print(f"backfill batch: {cfg.backfill_date}, daily batches: {cfg.first_replay_date} .. {cfg.last_replay_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. From one order row to a CDC stream / 从一行订单到 CDC 变更流 / 1行の注文から CDC ストリームへ
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC The Olist orders file has one row per order with a timestamp per stage. `build_orders_cdc` explodes it into **one row per status change** (an after-image), sorted by time and numbered with `change_seq`. Each change only shows the timestamps known at that moment, and its `_delivery_date` is the day the change happened. Anything before the backfill date arrives in the single backfill batch.
# MAGIC
# MAGIC Order `o3` is copied from a real Olist row: its carrier date is **months before the purchase**. Sorted by time, "shipped" becomes change 1 and happens before the order exists. That is why silver has the rule `change_before_purchase` (notebook 05).
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC Olist 的订单文件每个订单一行，每个阶段一个时间戳。`build_orders_cdc` 把它展开成**每次状态变更一行**（变更后镜像），按时间排序并用 `change_seq` 编号。每条变更只显示当时已知的时间戳，`_delivery_date` 就是这次变更发生的那天。回填日期之前的变更都放进唯一的回填批次。
# MAGIC
# MAGIC 订单 `o3` 照搬了 Olist 里的一行真实数据：它的发货时间比下单时间**早了好几个月**。按时间排序后，"已发货"变成了第 1 条变更，发生在订单存在之前。这就是 Silver 里有 `change_before_purchase` 规则的原因（见 notebook 05）。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC Olist の注文ファイルは1注文1行で、段階ごとにタイムスタンプがあります。`build_orders_cdc` はこれを**ステータス変更ごとに1行**（変更後イメージ）へ展開し、時刻順に並べて `change_seq` で番号を付けます。各変更には、その時点で分かっているタイムスタンプだけが入り、`_delivery_date` は変更が起きた日になります。バックフィル日より前の変更は、1つのバックフィルバッチにまとめて届きます。
# MAGIC
# MAGIC 注文 `o3` は Olist の実データの1行をそのまま使っています。発送日時が購入日時より**数か月も前**です。時刻順に並べると「発送済み」が変更1になり、注文が存在する前に起きたことになります。Silver に `change_before_purchase` ルールがあるのはこのためです（ノートブック 05）。

# COMMAND ----------

raw_orders = spark.createDataFrame(
    [
        # delivered normally, partly before and partly after the backfill date
        ("o1", "c1", "delivered", "2018-05-30 10:00:00", "2018-05-30 10:20:00", "2018-05-31 15:00:00", "2018-06-04 18:00:00", "2018-06-12 00:00:00"),
        # approved, then cancelled: the final status is added one hour after the last timestamp
        ("o2", "c2", "canceled", "2018-06-02 08:00:00", "2018-06-02 09:00:00", None, None, "2018-06-20 00:00:00"),
        # real Olist row (7c48bb55...): carrier date 2018-01-26 for a purchase on 2018-07-16
        ("o3", "c3", "delivered", "2018-07-16 18:40:53", "2018-07-16 18:50:22", "2018-01-26 13:35:00", "2018-07-23 20:04:45", "2018-08-07 00:00:00"),
    ],
    "order_id string, customer_id string, order_status string, order_purchase_timestamp string, order_approved_at string, "
    "order_delivered_carrier_date string, order_delivered_customer_date string, order_estimated_delivery_date string",
)
cdc = replay.build_orders_cdc(raw_orders, cfg)
show(
    cdc.select("order_id", "change_seq", "order_status", "change_ts", "order_purchase_ts", "order_delivered_customer_ts", "_delivery_date")
    .orderBy("order_id", "change_seq")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Injecting anomalies / 注入异常 / 異常の注入
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC `inject_item_anomalies` tampers with order items on replay days only (never the backfill). Each row gets a bucket from `xxhash64(seed, key) % 10000`, and each anomaly owns a **disjoint bucket range**. Two consequences: the same seed always produces the same dirty data, and no row gets two anomalies, so every count can be checked exactly.
# MAGIC
# MAGIC Below are 5,000 clean items spread over the ten replay days. After injection, count the tags.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC `inject_item_anomalies` 只在回放日篡改订单明细，从不动回填批次。每行用 `xxhash64(seed, key) % 10000` 算出一个桶号，每种异常占用**互不重叠的桶区间**。由此有两个结果：同一个种子总是产生同样的脏数据；一行不会同时带两种异常，所以每个数字都能精确核对。
# MAGIC
# MAGIC 下面是分布在 10 个回放日上的 5,000 条干净明细。注入之后，统计一下各类标签。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC `inject_item_anomalies` はリプレイ日の注文明細だけを改変し、バックフィルには手を付けません。各行は `xxhash64(seed, key) % 10000` でバケット番号を得て、異常の種類ごとに**重ならないバケット範囲**を持ちます。その結果、同じシードからは常に同じ汚れたデータが生まれ、1行に2つの異常が付くこともないため、すべての件数を正確に検証できます。
# MAGIC
# MAGIC 以下は10日間のリプレイ期間に分散した5,000件のきれいな明細です。注入後、タグごとに件数を数えます。

# COMMAND ----------

clean_items = spark.range(5000).select(
    F.concat(F.lit("o"), F.col("id").cast("string")).alias("order_id"),
    F.lit(1).alias("order_item_id"),
    F.lit("p1").alias("product_id"),
    F.lit("s1").alias("seller_id"),
    F.expr("timestamp'2018-06-01 12:00:00' + make_interval(0, 0, 0, cast(id % 10 as int))").alias("order_purchase_ts"),
    F.lit(None).cast("timestamp").alias("shipping_limit_ts"),
    F.lit(100).cast("decimal(12,2)").alias("price"),
    F.lit(10).cast("decimal(12,2)").alias("freight_value"),
).withColumn("_delivery_date", F.to_date("order_purchase_ts"))

dirty = replay.inject_item_anomalies(clean_items, cfg)
print(f"clean rows: {clean_items.count()}, rows delivered after injection: {dirty.count()}")
show(dirty.groupBy("_anomaly").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. What each anomaly looks like / 每种异常长什么样 / 各異常の中身
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC One example per tag. Look at what changed compared with a clean row (price 100.00, product `p1`, delivered on the purchase day): a negative price, a missing or unknown product, a delivery day moved later, or the same key twice. From day 6 on, the producer also starts sending a new field, `discount_amount` (notebook 02).
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 每种标签举一个例子。和干净的行（价格 100.00、商品 `p1`、在下单当天投递）对比，看哪里变了：价格变成负数、商品缺失或不存在、投递日期往后推，或者同一个键出现两次。从第 6 天开始，上游还会多发一个新字段 `discount_amount`（见 notebook 02）。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC タグごとに1例を表示します。きれいな行（価格 100.00、商品 `p1`、購入日当日に配信）と比べて何が変わったかを見てください。価格が負になる、商品が欠落または存在しない、配信日が後ろにずれる、同じキーが2回現れる、のいずれかです。6日目からは、送信元が新しいフィールド `discount_amount` も送り始めます（ノートブック 02）。

# COMMAND ----------

examples = (
    dirty.where(F.col("_anomaly").isNotNull())
    .withColumn("_rn", F.row_number().over(Window.partitionBy("_anomaly").orderBy("order_id")))
    .where("_rn = 1")
    .select("_anomaly", "order_id", "product_id", "price", F.to_date("order_purchase_ts").alias("sold_on"), "_delivery_date", "discount_amount")
    .orderBy("_anomaly")
)
show(examples)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try it yourself / 自己试试 / やってみよう
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC 1. Rerun section 2 and confirm the counts are identical. Then pass `Config(..., seed=7)`: the counts change, but stay identical across reruns with seed 7.
# MAGIC 2. Pass `poison_date=dt.date(2018, 6, 6)` to `Config`. A new tag `poison_invalid_price` appears on about 12% of that day's rows. This is how the e2e test makes the quality gate fail (notebook 05).
# MAGIC 3. In section 1, remove the approved timestamp of `o2`. How many changes does the order get now?
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 1. 重跑第 2 节，确认数字完全一样。然后传入 `Config(..., seed=7)`：数字会变，但用种子 7 反复运行，结果依然一致。
# MAGIC 2. 给 `Config` 传入 `poison_date=dt.date(2018, 6, 6)`。那一天约 12% 的行会多出新标签 `poison_invalid_price`。端到端测试就是用这个方法让质量闸门失败的（见 notebook 05）。
# MAGIC 3. 在第 1 节里去掉 `o2` 的审核时间戳。这个订单现在有几条变更？
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 1. 2節を再実行し、件数がまったく同じことを確認してください。次に `Config(..., seed=7)` を渡すと件数は変わりますが、シード7で何度実行しても同じになります。
# MAGIC 2. `Config` に `poison_date=dt.date(2018, 6, 6)` を渡してください。その日の行の約12%に新しいタグ `poison_invalid_price` が付きます。E2E テストはこの方法で品質ゲートを失敗させています（ノートブック 05）。
# MAGIC 3. 1節で `o2` の承認日時を削除してください。この注文の変更は何件になりますか。

# COMMAND ----------

# MAGIC %md
# MAGIC ## In the interview / 面试时怎么说 / 面接では
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC > "Olist is static, so I wrote a replay that delivers it as daily files and injects the problems real feeds have: duplicates, late rows, invalid values, out-of-order CDC, a new field and seller relocations. Injection is hash-based with disjoint buckets, so it is deterministic and every anomaly is recorded in a manifest. That manifest is my ground truth: the tests check that the pipeline's own metrics report exactly what was injected."
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC > "Olist 是静态数据，所以我写了一个回放程序，把它变成每天投递的文件，并注入真实数据源常见的问题：重复、迟到、非法值、乱序的 CDC、新字段和卖家迁址。注入基于哈希、桶区间互不重叠，所以结果是确定的，每个异常都记录在 manifest 里。这个 manifest 就是标准答案：测试会检查管道自己的指标报告的数量和注入的完全一致。"
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC > 「Olist は静的なデータなので、日次ファイルとして配信し、実際のフィードでよく起きる問題（重複、遅延、不正値、順序が逆転した CDC、新しいフィールド、セラーの移転）を注入するリプレイを書きました。注入はハッシュベースでバケット範囲が重ならないため決定的で、すべての異常がマニフェストに記録されます。このマニフェストが正解データで、パイプライン自身のメトリクスが注入した件数とちょうど一致することをテストで確認しています。」
