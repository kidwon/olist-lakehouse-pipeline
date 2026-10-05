# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Silver: de-duplication and forward-only CDC / 去重与只前进的 CDC / 重複排除と前進のみの CDC
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC Two things go wrong with order data on the way in. **Order items** are re-sent: twice in one file, or again the next day. If a duplicate reaches the fact table, revenue is double counted. **Order status changes** (CDC) arrive out of order: `delivered` can land before `shipped`. If the last row to arrive simply wins, a delivered order goes back to shipped.
# MAGIC
# MAGIC This notebook runs the real functions from `src/olist_pipeline/silver.py` against both problems. Design decisions: ADR-0001 (dedup) and ADR-0002 (CDC).
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 订单数据在进入管道时会出两类问题。**订单明细**会被重复发送：同一个文件里出现两次，或者第二天再发一次。重复数据一旦进入事实表，销售额就会被重复计算。**订单状态变更**（CDC）会乱序到达：`delivered` 可能比 `shipped` 先到。如果简单地"最后到的覆盖前面的"，已送达的订单就会退回"已发货"。
# MAGIC
# MAGIC 这个 notebook 用 `src/olist_pipeline/silver.py` 里的真实函数来处理这两个问题。设计决策见 ADR-0001（去重）和 ADR-0002（CDC）。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 注文データは取り込みの途中で2種類の問題を起こします。**注文明細**は再送されます。同じファイルに2回含まれたり、翌日にもう一度届いたりします。重複がファクトテーブルに入ると、売上が二重計上されます。**注文ステータスの変更**（CDC）は順序どおりに届きません。`delivered` が `shipped` より先に届くことがあります。「最後に届いた行が勝つ」だけでは、配達済みの注文が発送済みに戻ってしまいます。
# MAGIC
# MAGIC このノートブックでは、`src/olist_pipeline/silver.py` の本物の関数を使って両方の問題を扱います。設計判断は ADR-0001（重複排除）と ADR-0002（CDC）を参照してください。

# COMMAND ----------

from datetime import datetime

from pyspark.sql import functions as F

from walkthrough_setup import fresh_table, setup
from olist_pipeline import silver

spark, SCHEMA = setup()
show = globals().get("display") or (lambda df: df.show(truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Duplicates inside one batch / 同一批次内的重复 / 同一バッチ内の重複
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC `keep_first` keeps one row per business key `(order_id, order_item_id)` with `row_number()` over a window. The order (`_ingested_at`) only decides *which* copy survives; for true duplicates both copies are identical anyway.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC `keep_first` 用窗口函数 `row_number()`，按业务键 `(order_id, order_item_id)` 每组只保留一行。排序（`_ingested_at`）只决定留下*哪一份*；对真正的重复来说，两份内容本来就一样。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC `keep_first` はウィンドウ上の `row_number()` を使い、業務キー `(order_id, order_item_id)` ごとに1行だけ残します。並び順（`_ingested_at`）は*どちらの*コピーを残すかを決めるだけで、本当の重複なら中身はどちらも同じです。

# COMMAND ----------

batch = spark.createDataFrame(
    [
        ("o1", 1, 100.0, datetime(2018, 6, 1, 9, 0)),
        ("o1", 1, 100.0, datetime(2018, 6, 1, 9, 0)),  # sent twice in the same file
        ("o1", 2, 30.0, datetime(2018, 6, 1, 9, 0)),
        ("o2", 1, 55.0, datetime(2018, 6, 1, 9, 0)),
    ],
    "order_id string, order_item_id int, price double, _ingested_at timestamp",
)
deduped = silver.keep_first(batch, ["order_id", "order_item_id"], [F.col("_ingested_at")])
print(f"before: {batch.count()} rows, after: {deduped.count()} rows")
show(deduped.orderBy("order_id", "order_item_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Re-delivery in a later batch / 后续批次的重复投递 / 後のバッチでの再送
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC A window only sees one batch. A line that was already loaded yesterday has to be caught against the **table**, so `merge_insert_new` uses a MERGE with only `WHEN NOT MATCHED THEN INSERT`. A key we already hold is a re-delivery, never an update. Run it twice and the revenue stays the same.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 窗口函数只能看到一个批次。昨天已经加载过的明细，必须和**表**比对才能发现，所以 `merge_insert_new` 用的 MERGE 只有 `WHEN NOT MATCHED THEN INSERT`。已经存在的键视为重复投递，绝不当作更新。连续执行两次，销售额也不会变。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC ウィンドウ関数は1つのバッチしか見えません。昨日すでに取り込んだ明細は**テーブル**と突き合わせて見つける必要があるため、`merge_insert_new` は `WHEN NOT MATCHED THEN INSERT` だけの MERGE を使います。既に持っているキーは再送であり、更新としては扱いません。2回実行しても売上は変わりません。

# COMMAND ----------

ITEMS = fresh_table(spark, SCHEMA, "demo_order_items")
schema = "order_id string, order_item_id int, price double"

day1 = spark.createDataFrame([("o1", 1, 100.0), ("o1", 2, 30.0)], schema)
day2 = spark.createDataFrame([("o1", 1, 100.0), ("o3", 1, 70.0)], schema)  # o1/1 is re-sent

for name, rows in [("day 1", day1), ("day 2", day2), ("day 2 again (job retry)", day2)]:
    silver.merge_insert_new(spark, ITEMS, rows, ["order_id", "order_item_id"])
    t = spark.table(ITEMS)
    print(f"after {name}: {t.count()} rows, revenue = {t.agg(F.sum('price')).first()[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Several status changes in one batch / 同一批次内的多个状态变更 / 同一バッチ内の複数のステータス変更
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC One order can move through several statuses inside one daily file, in any row order. `latest_change_per_order` keeps the row with the **highest `change_seq`**, the producer's sequence number. Arrival order and timestamps are not used (why: see the interview note at the end).
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 同一个订单可能在一份每日文件里经历好几个状态，行的顺序也是任意的。`latest_change_per_order` 保留 **`change_seq` 最大**的那一行，也就是上游系统给出的序号。不使用到达顺序或时间戳（原因见最后的面试部分）。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 1つの注文が、1日分のファイルの中で複数のステータスを経ることがあり、行の順序もばらばらです。`latest_change_per_order` は **`change_seq` が最大**の行、つまり送信元が付けた連番が最大の行を残します。到着順やタイムスタンプは使いません（理由は最後の面接メモを参照）。

# COMMAND ----------

changes_schema = "order_id string, order_status string, change_seq int, _ingested_at timestamp"
t0 = datetime(2018, 6, 1, 9, 0)
one_file = spark.createDataFrame(
    [("o1", "delivered", 4, t0), ("o1", "created", 1, t0), ("o1", "shipped", 3, t0), ("o2", "approved", 2, t0)],
    changes_schema,
)
show(silver.latest_change_per_order(one_file).orderBy("order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Out-of-order changes across batches / 跨批次的乱序变更 / バッチをまたぐ順序の逆転
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC `merge_cdc_forward_only` updates a row only `WHEN MATCHED AND s.change_seq > t.change_seq`. Below, the order is approved (seq 2), then delivered (seq 4). Two days later the older `shipped` change (seq 3) finally arrives, and then `delivered` is sent again. The table never goes backwards.
# MAGIC
# MAGIC The Delta history at the end shows how many rows each MERGE actually updated: only the two changes that moved the order forward did anything.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC `merge_cdc_forward_only` 只在 `WHEN MATCHED AND s.change_seq > t.change_seq` 时才更新。下面的例子里，订单先审核通过（seq 2），再送达（seq 4）。两天后，更早的 `shipped` 变更（seq 3）才到，接着 `delivered` 又被重发了一次。表里的状态始终不会倒退。
# MAGIC
# MAGIC 最后的 Delta 历史显示了每次 MERGE 实际更新了几行：只有让订单向前推进的那两次变更起了作用。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC `merge_cdc_forward_only` は `WHEN MATCHED AND s.change_seq > t.change_seq` のときだけ更新します。下の例では、注文がまず承認され（seq 2）、次に配達されます（seq 4）。2日後に古い `shipped` の変更（seq 3）がようやく届き、さらに `delivered` が再送されます。テーブルの状態は一度も後退しません。
# MAGIC
# MAGIC 最後の Delta 履歴で、各 MERGE が実際に何行更新したかを確認できます。注文を前に進めた2回の変更だけが効いています。

# COMMAND ----------

ORDERS = fresh_table(spark, SCHEMA, "demo_orders")


def arrive(label, status, seq):
    latest = silver.latest_change_per_order(spark.createDataFrame([("o1", status, seq, t0)], changes_schema))
    silver.merge_cdc_forward_only(spark, ORDERS, latest)
    row = spark.table(ORDERS).first()
    print(f"{label:<34} arrives {status:<10} seq {seq} -> table: {row.order_status} (seq {row.change_seq})")


arrive("batch 2018-06-01", "approved", 2)
arrive("batch 2018-06-02", "delivered", 4)
arrive("batch 2018-06-04 (late, older)", "shipped", 3)
arrive("batch 2018-06-05 (re-sent)", "delivered", 4)

# COMMAND ----------

history = (
    spark.sql(f"DESCRIBE HISTORY {ORDERS}")
    .where("operation = 'MERGE'")
    .select(
        "version",
        F.col("operationMetrics.numSourceRows").alias("source_rows"),
        F.col("operationMetrics.numTargetRowsInserted").alias("inserted"),
        F.col("operationMetrics.numTargetRowsUpdated").alias("updated"),
    )
    .orderBy("version")
)
show(history)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. One statement, four behaviours / 一条语句，四种写法 / 1つの文、4つの使い方
# MAGIC
# MAGIC | Case / 场景 / ケース | Function | MERGE clauses | Upsert? |
# MAGIC |---|---|---|---|
# MAGIC | Customers, reviews / 客户、评论 / 顧客、レビュー | `merge_upsert` | matched → update, not matched → insert | yes |
# MAGIC | Order items / 订单明细 / 注文明細 | `merge_insert_new` | not matched → insert only | no: insert-only |
# MAGIC | Order CDC / 订单 CDC / 注文 CDC | `merge_cdc_forward_only` | matched **and newer** → update, not matched → insert | conditional |
# MAGIC | Seller SCD2 / 卖家 SCD2 / セラー SCD2 | `scd2.apply_snapshot` (04) | matched → close old version, not matched → insert new version | no |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try it yourself / 自己试试 / やってみよう
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC 1. In section 4, change the condition by calling `t.alias("t").merge(...).whenMatchedUpdateAll()` without the `change_seq` condition (copy `merge_cdc_forward_only` into a cell). Rerun: the late `shipped` row now overwrites `delivered`. That is the bug this design prevents.
# MAGIC 2. In section 2, replace `merge_insert_new` with `silver.merge_upsert`. The row count stays correct, but every re-delivery now rewrites the row. Why does that matter for an append-only feed?
# MAGIC 3. Give two changes the same `change_seq` in section 3. Which one survives, and why is a sequence the producer guarantees to be unique important?
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 1. 在第 4 节里，把 `merge_cdc_forward_only` 复制到一个单元格，去掉 `change_seq` 条件，直接用 `whenMatchedUpdateAll()`。重跑后，迟到的 `shipped` 会覆盖 `delivered`。这正是这个设计要防止的 bug。
# MAGIC 2. 在第 2 节里，把 `merge_insert_new` 换成 `silver.merge_upsert`。行数仍然正确，但每次重复投递都会重写那一行。对只追加的数据源来说，为什么这一点很重要？
# MAGIC 3. 在第 3 节里给两条变更相同的 `change_seq`。留下的是哪一条？为什么上游保证序号唯一很重要？
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 1. 4節で `merge_cdc_forward_only` をセルにコピーし、`change_seq` の条件を外して `whenMatchedUpdateAll()` にしてください。再実行すると、遅れて届いた `shipped` が `delivered` を上書きします。これがこの設計で防いでいるバグです。
# MAGIC 2. 2節で `merge_insert_new` を `silver.merge_upsert` に置き換えてください。行数は正しいままですが、再送のたびに行が書き直されます。追記専用のフィードでは、なぜそれが問題になるのでしょうか。
# MAGIC 3. 3節で2つの変更に同じ `change_seq` を付けてください。どちらが残りますか。送信元が連番の一意性を保証することはなぜ重要でしょうか。

# COMMAND ----------

# MAGIC %md
# MAGIC ## In the interview / 面试时怎么说 / 面接では
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC > "Status changes arrive out of order, so I apply them forward-only: the MERGE updates a row only when the incoming `change_seq` is higher than the stored one. I use the producer's sequence rather than timestamps, because timestamps tie, clocks drift, and they can simply be wrong: in the real Olist data 166 orders have a carrier date before the purchase. A replay test delays some changes by two days and compares every order's final state with the ground truth."
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC > "状态变更会乱序到达，所以我只让它往前走：只有新来的 `change_seq` 比表里的大，MERGE 才更新。我用的是上游的序号而不是时间戳，因为时间戳会相同、时钟会有偏差，而且可能直接是错的：真实的 Olist 数据里有 166 个订单的发货时间早于下单时间。回放测试会把一部分变更推迟两天送达，再把每个订单的最终状态和标准答案逐条比对。"
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC > 「ステータス変更は順不同で届くため、前進方向にのみ適用しています。MERGE は、届いた `change_seq` が保存済みの値より大きいときだけ行を更新します。タイムスタンプではなく送信元の連番を使うのは、タイムスタンプは同値になり、時計はずれ、そもそも間違っていることもあるからです。実際の Olist データでは、166件の注文で発送日時が購入日時より前になっていました。リプレイのテストでは一部の変更を2日遅らせて配信し、全注文の最終状態を正解データと突き合わせています。」
